//! Create new `Streams` connected to external inputs.

use crate::Data;
use crate::dataflow::{Stream, ScopeParent, Scope};
use crate::dataflow::operators::core::{Input as InputCore};

// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait Input : Scope {
    /// Create a new `Stream` and `Handle` through which to supply input.
    ///
    /// The `new_input` method returns a pair `(Handle, Stream)` where the `Stream` can be used
    /// immediately for timely dataflow construction, and the `Handle` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `Handle` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input();
    ///         stream.inspect(|x| println!("hello {:?}", x));
    ///         input
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn new_input<D: Data>(&mut self) -> (Handle<<Self as ScopeParent>::Timestamp, D>, Stream<Self, D>);

    /// Create a new stream from a supplied interactive handle.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate
    /// if it as attached to more than one stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn input_from<D: Data>(&mut self, handle: &mut Handle<<Self as ScopeParent>::Timestamp, D>) -> Stream<Self, D>;
}

use crate::order::TotalOrder;
impl<G: Scope> Input for G where <G as ScopeParent>::Timestamp: TotalOrder {
    fn new_input<D: Data>(&mut self) -> (Handle<<G as ScopeParent>::Timestamp, D>, Stream<G, D>) {
        InputCore::new_input(self)
    }

    fn input_from<D: Data>(&mut self, handle: &mut Handle<<G as ScopeParent>::Timestamp, D>) -> Stream<G, D> {
        InputCore::input_from(self, handle)
    }
}

/// A handle to an input `Stream`, used to introduce data to a timely dataflow computation.
pub type Handle<T, D> = crate::dataflow::operators::core::input::Handle<T, Vec<D>>;
