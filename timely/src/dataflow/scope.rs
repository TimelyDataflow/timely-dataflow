//! A dataflow scope, used to build dataflow graphs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::activate::Activations;
use crate::progress::{Timestamp, Operate, Subgraph, SubgraphBuilder};
use crate::progress::{Source, Target};
use crate::progress::timestamp::Refines;
use crate::order::Product;
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;
use crate::worker::Worker;

/// Type alias for an iterative scope.
pub type Iterative<'scope, TOuter, TInner> = Scope<'scope, Product<TOuter, TInner>>;

/// A `Scope` manages the creation of new dataflow scopes, of operators and edges between them.
///
/// This is a shared object that can be freely cloned. It manages the scope construction through
/// a `RefCell`-wrapped subgraph builder, and all of this types methods use but do not hold write
/// access through the `RefCell`.
pub struct Scope<'scope, T: Timestamp> {
    /// The subgraph under assembly.
    ///
    /// Stored as `Rc<RefCell<...>>` so that multiple `Scope` clones can work on the same subgraph.
    /// All methods on this type must release their borrow on this field before returning.
    pub(crate) subgraph: &'scope RefCell<SubgraphBuilder<T>>,
    /// A copy of the worker hosting this scope.
    pub(crate) worker:   Worker,
    /// The log writer for this scope.
    pub(crate) logging:  Option<Logger>,
    /// The progress log writer for this scope.
    pub(crate) progress_logging:  Option<ProgressLogger<T>>,
}

impl<'scope, T: Timestamp> Scope<'scope, T> {
    /// Access to the underlying worker.
    pub fn worker(&self) -> &Worker { &self.worker }
    /// This worker's index out of `0 .. self.peers()`.
    pub fn index(&self) -> usize { self.worker.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.worker.peers() }
    /// Provides a shared handle to the activation scheduler.
    pub fn activations(&self) -> Rc<RefCell<Activations>> { self.worker.activations() }
    /// Constructs an `Activator` tied to the specified operator address.
    pub fn activator_for(&self, path: Rc<[usize]>) -> crate::scheduling::Activator { self.worker.activator_for(path) }

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
    pub fn reserve_operator(&self) -> OperatorSlot<'scope, T> {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let identifier = self.worker().new_identifier();
        OperatorSlot {
            scope: self.clone(),
            index,
            identifier,
            installed: false,
        }
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
        let (result, subgraph, slot) = self.scoped_raw(name, func);
        slot.install(Box::new(subgraph));
        result
    }

    /// Creates a dataflow subgraph, runs a user closure, and returns a result and the to-be-assembled parts.
    ///
    /// The returned subgraph must be registered in the operator slot.
    pub fn scoped_raw<T2, R, F>(&self, name: &str, func: F) -> (R, Subgraph<T, T2>, OperatorSlot<'scope, T>)
    where
        T2: Timestamp + Refines<T>,
        F: FnOnce(&mut Scope<T2>) -> R,
    {
        let parent = self.clone();
        let slot = parent.reserve_operator();
        let path = slot.addr();
        let identifier = slot.identifier();

        let type_name = std::any::type_name::<T2>();
        let progress_logging = parent.worker().logger_for(&format!("timely/progress/{type_name}"));
        let summary_logging  = parent.worker().logger_for(&format!("timely/summary/{type_name}"));

        let subgraph = RefCell::new(SubgraphBuilder::new_from(
            path, identifier, self.worker().logging(), summary_logging, name,
        ));

        let mut child = Scope {
            subgraph: &subgraph,
            worker: parent.worker.clone(),
            logging: parent.logging.clone(),
            progress_logging,
        };

        let result = func(&mut child);
        drop(child);
        let subgraph = subgraph.into_inner().build(&parent.worker);
        (result, subgraph, slot)
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

impl<'scope, T: Timestamp> Clone for Scope<'scope, T> {
    fn clone(&self) -> Self {
        Scope {
            subgraph: self.subgraph,
            worker: self.worker.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone(),
        }
    }
}

impl<'scope, T: Timestamp> std::fmt::Debug for Scope<'scope, T> {
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
pub struct OperatorSlot<'scope, T: Timestamp> {
    scope: Scope<'scope, T>,
    index: usize,
    identifier: usize,
    installed: bool,
}

impl<'scope, T: Timestamp> OperatorSlot<'scope, T> {
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
        // TODO: Check paths of self and operator; Operate has no such method at the moment.
        self.scope.subgraph.borrow_mut().add_child(operator, self.index, self.identifier);
        self.installed = true;
    }
}

impl<'scope, T: Timestamp> Drop for OperatorSlot<'scope, T> {
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

