//! Extension trait and implementation for observing and action on streamed data.

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

/// Methods to inspect records and batches of records on a stream.
pub trait Inspect<G: Scope, D: Data> {
    /// Runs a supplied closure on each observed data element.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn inspect<F: FnMut(&D)+'static>(&self, func: F) -> Self;
    /// Runs a supplied closure on each observed data batch (time and data slice).
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_batch(|t,xs| println!("seen at: {:?}\t{:?} records", t, xs.len()));
    /// });
    /// ```
    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Self;
}

impl<G: Scope, D: Data> Inspect<G, D> for Stream<G, D> {

    fn inspect<F: FnMut(&D)+'static>(&self, mut func: F) -> Stream<G, D> {
        self.unary_stream(Pipeline, "Inspect", move |input, output| {
            input.for_each(|time, data| {
                for datum in data.iter() { func(datum); }
                output.session(&time).give_content(data);
            });
        })
    }

    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Stream<G, D> {
        self.unary_stream(Pipeline, "InspectBatch", move |input, output| {
            input.for_each(|time, data| {
                func(&time, &data[..]);
                output.session(&time).give_content(data);
            });
        })
    }
}
