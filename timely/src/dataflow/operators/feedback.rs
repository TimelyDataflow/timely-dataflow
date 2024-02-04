//! Create cycles in a timely dataflow graph.

use crate::Data;

use crate::progress::Timestamp;
use crate::dataflow::{Scope, Stream};
use crate::dataflow::operators::core::{Feedback as FeedbackCore};

/// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
pub trait Feedback<G: Scope> {
    /// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
    ///
    /// The resulting `Stream` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Data passed through the stream will have their
    /// timestamps advanced by `summary`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Feedback, ConnectLoop, ToStream, Concat, Inspect, BranchWhen};
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     let (handle, cycle) = scope.feedback(1);
    ///     (0..10).to_stream(scope)
    ///            .concat(&cycle)
    ///            .inspect(|x| println!("seen: {:?}", x))
    ///            .branch_when(|t| t < &100).1
    ///            .connect_loop(handle);
    /// });
    /// ```
    fn feedback<D: Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, D>, Stream<G, D>);
}

impl<G: Scope> Feedback<G> for G {
    fn feedback<D: Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, D>, Stream<G, D>) {
        FeedbackCore::feedback(self, summary)
    }
}

/// A `Handle` specialized for using `Vec` as container
pub type Handle<G, D> = crate::dataflow::operators::core::feedback::Handle<G, Vec<D>>;
