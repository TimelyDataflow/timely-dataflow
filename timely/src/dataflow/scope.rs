//! A dataflow scope, used to build dataflow graphs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::communication::{Exchangeable, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::Scheduler;
use crate::scheduling::activate::Activations;
use crate::progress::{Timestamp, Operate, Subgraph, SubgraphBuilder};
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

    /// Connects a source of data with a target of the data. This only links the two for
    /// the purposes of tracking progress, rather than effect any data movement itself.
    pub fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    /// Reserves a slot for an operator in this scope.
    ///
    /// The returned [`OperatorSlot`] carries the scope-local index and worker-unique
    /// identifier of the future operator. It must be consumed by [`OperatorSlot::install`]
    /// before being dropped; otherwise it will panic, since the scope expects every
    /// reserved slot to eventually be filled.
    pub fn reserve_operator(&mut self) -> OperatorSlot<T> {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let identifier = self.new_identifier();
        OperatorSlot {
            scope: self.clone(),
            index,
            identifier,
            installed: false,
        }
    }

    /// Begins a nested subscope with a refining timestamp `T2`.
    ///
    /// Returns a freshly-allocated child [`Scope<T2>`] for building the inner
    /// dataflow, paired with an [`OperatorSlot`] reserved for the subgraph in
    /// this (parent) scope. After populating the child scope, finalize it with
    /// [`Scope::into_subgraph`] (passing the slot) and install the resulting
    /// [`Subgraph`] (or any wrapper around it) into the slot.
    ///
    /// The standard `scoped` / `region` / `iterative` methods are thin wrappers
    /// around `new_subscope` that install the built subgraph directly. Direct
    /// callers of `new_subscope` may interpose, e.g. by wrapping the built
    /// subgraph in a decorator that intercepts scheduling.
    pub fn new_subscope<T2>(&self, name: &str) -> (Scope<T2>, OperatorSlot<T>)
    where
        T2: Timestamp + Refines<T>,
    {
        let mut parent = self.clone();
        let slot = parent.reserve_operator();
        let path = slot.addr();
        let identifier = slot.identifier();

        let type_name = std::any::type_name::<T2>();
        let progress_logging = parent.logger_for(&format!("timely/progress/{type_name}"));
        let summary_logging  = parent.logger_for(&format!("timely/summary/{type_name}"));

        let child = Scope {
            subgraph: Rc::new(RefCell::new(SubgraphBuilder::new_from(
                path, identifier, self.logging(), summary_logging, name,
            ))),
            worker: parent.worker.clone(),
            logging: parent.logging.clone(),
            progress_logging,
        };

        (child, slot)
    }

    /// Finalizes this child subscope into a [`Subgraph`] registered against `parent`.
    ///
    /// `self` must be a child [`Scope<T>`] obtained from a `new_subscope` call on
    /// `parent` (or a clone of it). After this returns, the caller is responsible
    /// for installing the resulting `Subgraph` (or any wrapper around it) into the
    /// `OperatorSlot` returned alongside this scope.
    ///
    /// This method should not be called other than on the last remaining instance
    /// of the scope. If a clone of `self` exists, this code will panic as it tries
    /// to unwrap a shared `Rc` reference.
    pub fn build<TOuter>(self, parent: &mut Scope<TOuter>) -> Subgraph<TOuter, T>
    where
        TOuter: Timestamp,
        T: Refines<TOuter>,
    {
        let Scope { subgraph: builder, .. } = self;
        Rc::try_unwrap(builder)
            .map_err(|_| ())
            .expect(
                "Cannot consume subscope: an outstanding `Scope` clone is still alive. \
                 This usually means the child `Scope` was cloned and a clone was held \
                 past the build() call."
            )
            .into_inner()
            .build(parent)
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
        let (mut child, mut slot) = self.new_subscope::<T2>(name);
        let result = func(&mut child);
        let subgraph = child.build(slot.scope_mut());
        slot.install(Box::new(subgraph));
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

/// A reservation for an operator at a specific position in a parent [`Scope`].
///
/// Returned by [`Scope::reserve_operator`] and [`SubscopeHandle::build`]. The
/// slot carries the scope-local index and the worker-unique identifier of the
/// operator-to-be. It must be consumed by [`OperatorSlot::install`] before
/// being dropped; dropping an unfilled slot panics, since the parent scope
/// expects the reserved index to eventually be filled.
#[derive(Debug)]
pub struct OperatorSlot<T: Timestamp> {
    scope: Scope<T>,
    index: usize,
    identifier: usize,
    installed: bool,
}

impl<T: Timestamp> OperatorSlot<T> {
    /// The scope-local index reserved for the operator.
    pub fn index(&self) -> usize { self.index }

    /// The worker-unique identifier reserved for the operator (used for logging).
    pub fn identifier(&self) -> usize { self.identifier }

    /// The address (path from the worker root) at which the operator will live.
    pub fn addr(&self) -> Rc<[usize]> {
        let scope_path = &self.scope.subgraph.borrow().path[..];
        let mut addr = Vec::with_capacity(scope_path.len() + 1);
        addr.extend_from_slice(scope_path);
        addr.push(self.index);
        addr.into()
    }

    /// Installs `operator` at this slot, consuming the slot.
    pub fn install(mut self, operator: Box<dyn Operate<T>>) {
        self.scope.subgraph.borrow_mut().add_child(operator, self.index, self.identifier);
        self.installed = true;
    }

    /// Mutable access to the containing scope. Used to register a built [`Subgraph`]
    /// before [`OperatorSlot::install`].
    pub fn scope_mut(&mut self) -> &mut Scope<T> { &mut self.scope }
}

impl<T: Timestamp> Drop for OperatorSlot<T> {
    fn drop(&mut self) {
        if !self.installed && !std::thread::panicking() {
            panic!(
                "OperatorSlot for index {} dropped without `install` being called. \
                 Every reserved operator slot must be filled.",
                self.index,
            );
        }
    }
}

