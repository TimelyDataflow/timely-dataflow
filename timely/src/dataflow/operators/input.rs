//! Create new `Streams` connected to external inputs.

use crate::dataflow::operators::InputCore;
use crate::dataflow::operators::input_core::HandleCore;
use crate::dataflow::{Stream, ScopeParent, Scope};
use crate::order::TotalOrder;
use crate::Data;
use crate::progress::Timestamp;
use crate::communication::Container;

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

impl<G: Scope> Input for G where <G as ScopeParent>::Timestamp: TotalOrder {
    fn new_input<D: Data>(&mut self) -> (Handle<<G as ScopeParent>::Timestamp, D>, Stream<G, D>) {
        self.new_input_core()
    }

    fn input_from<D: Data>(&mut self, handle: &mut Handle<<G as ScopeParent>::Timestamp, D>) -> Stream<G, D> {
        self.input_core_from(handle)
    }
}

/// A handle to inputs producing `Vec<D>` containers.
pub type Handle<T, D> = HandleCore<T, Vec<D>>;
