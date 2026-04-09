//! A dataflow scope, used to build dataflow graphs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::communication::{Exchangeable, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::Scheduler;
use crate::scheduling::activate::Activations;
use crate::progress::{Timestamp, Operate, SubgraphBuilder};
use crate::progress::{Source, Target};
use crate::progress::timestamp::Refines;
use crate::order::Product;
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;
use crate::worker::{AsWorker, Config, Worker};

/// Type alias for an iterative scope.
pub type Iterative<TOuter, TInner> = Scope<Product<TOuter, TInner>>;

/// A `Scope` wraps a `SubgraphBuilder` and manages the addition
/// of `Operate`s and the connection of edges between them.
///
/// Importantly, this is a *shared* object, backed by `Rc<RefCell<>>` wrappers. Each method
/// takes a shared reference, but can be thought of as first calling `.clone()` and then calling the
/// method. Each method does not hold the `RefCell`'s borrow, and should prevent accidental panics.
pub struct Scope<T: Timestamp> {
    /// The subgraph under assembly.
    ///
    /// Stored as `Rc<RefCell<...>>` so that multiple `Scope` clones can share the
    /// same subgraph state during construction. The owning `scoped` / `region` /
    /// `dataflow` call recovers the inner `SubgraphBuilder` via `Rc::try_unwrap`
    /// when the closure returns; if a clone has escaped the closure, this fails
    /// loudly with an actionable panic message.
    pub(crate) subgraph: Rc<RefCell<SubgraphBuilder<T>>>,
    /// A copy of the worker hosting this scope.
    pub(crate) worker:   Worker,
    /// The log writer for this scope.
    pub(crate) logging:  Option<Logger>,
    /// The progress log writer for this scope.
    pub(crate) progress_logging:  Option<ProgressLogger<T>>,
}

impl<T: Timestamp> Scope<T> {
    /// This worker's index out of `0 .. self.peers()`.
    pub fn index(&self) -> usize { self.worker.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.worker.peers() }

    /// A useful name describing the scope.
    pub fn name(&self) -> String { self.subgraph.borrow().name.clone() }

    /// A sequence of scope identifiers describing the path from the worker root to this scope.
    pub fn addr(&self) -> Rc<[usize]> { Rc::clone(&self.subgraph.borrow().path) }

    /// A sequence of scope identifiers describing the path from the worker root to the child
    /// indicated by `index`.
    pub fn addr_for_child(&self, index: usize) -> Rc<[usize]> {
        let path = &self.subgraph.borrow().path[..];
        let mut addr = Vec::with_capacity(path.len() + 1);
        addr.extend_from_slice(path);
        addr.push(index);
        addr.into()
    }

    /// Connects a source of data with a target of the data. This only links the two for
    /// the purposes of tracking progress, rather than effect any data movement itself.
    pub fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    /// Adds a child `Operate` to this scope. Returns the new child's index.
    pub fn add_operator(&mut self, operator: Box<dyn Operate<T>>) -> usize {
        let index = self.allocate_operator_index();
        let global = self.new_identifier();
        self.add_operator_with_indices(operator, index, global);
        index
    }

    /// Allocates a new scope-local operator index.
    ///
    /// This method is meant for use with `add_operator_with_index`, which accepts a scope-local
    /// operator index allocated with this method. This method does cause the scope to expect that
    /// an operator will be added, and it is an error not to eventually add such an operator.
    pub fn allocate_operator_index(&mut self) -> usize {
        self.subgraph.borrow_mut().allocate_child_id()
    }

    /// Adds a child `Operate` to this scope using a supplied index.
    ///
    /// This is used internally when there is a gap between allocate a child identifier and adding the
    /// child, as happens in subgraph creation.
    pub fn add_operator_with_index(&mut self, operator: Box<dyn Operate<T>>, index: usize) {
        let global = self.new_identifier();
        self.add_operator_with_indices(operator, index, global);
    }

    /// Adds a child `Operate` to this scope using supplied indices.
    ///
    /// The two indices are the scope-local operator index, and a worker-unique index used for e.g. logging.
    pub fn add_operator_with_indices(&mut self, operator: Box<dyn Operate<T>>, local: usize, global: usize) {
        self.subgraph.borrow_mut().add_child(operator, local, global);
    }

    /// Creates a dataflow subgraph.
    ///
    /// This method allows the user to create a nested scope with any timestamp that
    /// "refines" the enclosing timestamp (informally: extends it in a reversible way).
    ///
    /// This is most commonly used to create new iterative contexts, and the provided
    /// method `iterative` for this task demonstrates the use of this method.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    /// use timely::order::Product;
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<Vec<String>>();
    ///         let output = child1.scoped::<Product<u64,u32>,_,_>("ScopeName", |child2| {
    ///             stream.enter(child2).leave(child1)
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    #[inline]
    pub fn scoped<T2, R, F>(&self, name: &str, func: F) -> R
    where
        T2: Timestamp + Refines<T>,
        F: FnOnce(&mut Scope<T2>) -> R,
    {
        let mut scope = self.clone();
        let index = scope.subgraph.borrow_mut().allocate_child_id();
        let identifier = scope.new_identifier();
        let path = scope.addr_for_child(index);

        let type_name = std::any::type_name::<T2>();
        let progress_logging = scope.logger_for(&format!("timely/progress/{type_name}"));
        let summary_logging  = scope.logger_for(&format!("timely/summary/{type_name}"));

        let subscope = Rc::new(RefCell::new(SubgraphBuilder::new_from(path, identifier, self.logging(), summary_logging, name)));
        let result = {
            let mut builder = Scope {
                subgraph: Rc::clone(&subscope),
                worker: scope.worker.clone(),
                logging: scope.logging.clone(),
                progress_logging,
            };
            func(&mut builder)
        };
        let subscope = Rc::try_unwrap(subscope)
            .map_err(|_| ())
            .expect(
                "Cannot consume scope: an outstanding `Scope` clone is still alive. \
                 This usually means a `Scope` was cloned and held past the scoped() / \
                 region() / iterative() / dataflow() call that constructed it."
            )
            .into_inner()
            .build(&mut scope);

        scope.add_operator_with_indices(Box::new(subscope), index, identifier);

        result
    }

    /// Creates an iterative dataflow subgraph.
    ///
    /// This method is a specialization of `scoped` which uses the `Product` timestamp
    /// combinator, suitable for iterative computations in which iterative development
    /// at some time cannot influence prior iterations at a future time.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<Vec<String>>();
    ///         let output = child1.iterative::<u32,_,_>(|child2| {
    ///             stream.enter(child2).leave(child1)
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    pub fn iterative<T2, R, F>(&self, func: F) -> R
    where
        T2: Timestamp,
        F: FnOnce(&mut Scope<Product<T, T2>>) -> R,
    {
        self.scoped::<Product<T, T2>, R, F>("Iterative", func)
    }

    /// Creates a dataflow region with the same timestamp.
    ///
    /// This method is a specialization of `scoped` which uses the same timestamp as the
    /// containing scope. It is used mainly to group regions of a dataflow computation, and
    /// provides some computational benefits by abstracting the specifics of the region.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<Vec<String>>();
    ///         let output = child1.region(|child2| {
    ///             stream.enter(child2).leave(child1)
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    pub fn region<R, F>(&self, func: F) -> R
    where
        F: FnOnce(&mut Scope<T>) -> R,
    {
        self.region_named("Region", func)
    }

    /// Creates a dataflow region with the same timestamp and a supplied name.
    ///
    /// This method is a specialization of `scoped` which uses the same timestamp as the
    /// containing scope. It is used mainly to group regions of a dataflow computation, and
    /// provides some computational benefits by abstracting the specifics of the region.
    ///
    /// This variant allows you to specify a name for the region, which can be read out in
    /// the timely logging streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<Vec<String>>();
    ///         let output = child1.region_named("region", |child2| {
    ///             stream.enter(child2).leave(child1)
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    pub fn region_named<R, F>(&self, name: &str, func: F) -> R
    where
        F: FnOnce(&mut Scope<T>) -> R,
    {
        self.scoped::<T, R, F>(name, func)
    }
}

impl<T: Timestamp> AsWorker for Scope<T> {
    fn config(&self) -> &Config { self.worker.config() }
    fn index(&self) -> usize { self.worker.index() }
    fn peers(&self) -> usize { self.worker.peers() }
    fn allocate<D: Exchangeable>(&mut self, identifier: usize, address: Rc<[usize]>) -> (Vec<Box<dyn Push<D>>>, Box<dyn Pull<D>>) { self.worker.allocate(identifier, address) }
    fn pipeline<D: 'static>(&mut self, identifier: usize, address: Rc<[usize]>) -> (ThreadPusher<D>, ThreadPuller<D>) { self.worker.pipeline(identifier, address) }
    fn broadcast<D: Exchangeable + Clone>(&mut self, identifier: usize, address: Rc<[usize]>) -> (Box<dyn Push<D>>, Box<dyn Pull<D>>) { self.worker.broadcast(identifier, address) }
    fn new_identifier(&mut self) -> usize { self.worker.new_identifier() }
    fn peek_identifier(&self) -> usize { self.worker.peek_identifier() }
    fn log_register(&self) -> Option<::std::cell::RefMut<'_, crate::logging_core::Registry>> { self.worker.log_register() }
}

impl<T: Timestamp> Scheduler for Scope<T> {
    fn activations(&self) -> Rc<RefCell<Activations>> { self.worker.activations() }
}

impl<T: Timestamp> Clone for Scope<T> {
    fn clone(&self) -> Self {
        Scope {
            subgraph: Rc::clone(&self.subgraph),
            worker: self.worker.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone(),
        }
    }
}

impl<T: Timestamp> std::fmt::Debug for Scope<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scope")
            .field("name", &self.subgraph.borrow().name)
            .field("path", &self.subgraph.borrow().path)
            .finish_non_exhaustive()
    }
}
