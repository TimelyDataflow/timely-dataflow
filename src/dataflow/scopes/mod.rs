//! Hierarchical organization of timely dataflow graphs.

use progress::{Timestamp, Operate};
use progress::nested::{Source, Target};
use logging::Logger;
use timely_communication::Allocate;

use std::rc::Rc;

pub mod root;
pub mod child;

pub use self::child::Child;
pub use self::root::Root;

/// The information a child scope needs from its parent.
pub trait ScopeParent: Allocate+Clone {
    /// The timestamp associated with data in this scope.
    type Timestamp : Timestamp;

    /// Allocates a new locally unique identifier.
    fn new_identifier(&mut self) -> usize;
}

/// The fundamental operations required to add and connect operators in a timely dataflow graph.
///
/// Importantly, this is often a *shared* object, backed by a `Rc<RefCell<>>` wrapper. Each method
/// takes a shared reference, but can be thought of as first calling .clone() and then calling the
/// method. Each method does not hold the `RefCell`'s borrow, and should prevent accidental panics.
pub trait Scope: ScopeParent {
    /// A useful name describing the scope.
    fn name(&self) -> String;

    /// A sequence of scope identifiers describing the path from the `Root` to this scope.
    fn addr(&self) -> Vec<usize>;

    /// Connects a source of data with a target of the data. This only links the two for
    /// the purposes of tracking progress, rather than effect any data movement itself.
    fn add_edge(&self, source: Source, target: Target);

    /// Adds a child `Operate` to the builder's scope. Returns the new child's index.
    fn add_operator<SC: Operate<Self::Timestamp>+'static>(&mut self, scope: SC) -> usize {
        let index = self.allocate_operator_index();
        self.add_operator_with_index(scope, index);
        index
    }

    /// Allocates a new operator index, for use with `add_operator_with_index`.
    fn allocate_operator_index(&mut self) -> usize;

    /// Adds a child `Operate` to the builder's scope using a supplied index.
    ///
    /// This is used interally when there is a gap between allocate a child identifier and adding the
    /// child, as happens in subgraph creation.
    fn add_operator_with_index<SC: Operate<Self::Timestamp>+'static>(&mut self, scope: SC, index: usize);

    /// Creates a `Subgraph` from a closure acting on a `Child` scope, and returning
    /// whatever the closure returns.
    ///
    /// Commonly used to create new timely dataflow subgraphs, either creating new input streams
    /// and the input handle, or ingressing data streams and returning the egresses stream.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<String>();
    ///         let output = child1.scoped::<u32,_,_>(|child2| {
    ///             stream.enter(child2).leave()
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    fn scoped<T: Timestamp, R, F:FnOnce(&mut Child<Self, T>)->R>(&mut self, func: F) -> R;

    /// TODO(andreal)
    fn logging(&self) -> Logger;
}
