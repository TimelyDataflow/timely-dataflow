//! Extension trait and implementation for observing and action on streamed data.

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::generic::unary::Unary;

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
    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, func: F) -> Self;
}

impl<G: Scope, D: Data> Inspect<G, D> for Stream<G, D> {

    fn inspect<F: FnMut(&D)+'static>(&self, mut func: F) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary_stream(Pipeline, "Inspect", move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                for datum in vector.iter() { func(datum); }
                output.session(&time).give_vec(&mut vector);
            });
        })
    }

    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary_stream(Pipeline, "InspectBatch", move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                func(&time, &vector[..]);
                output.session(&time).give_vec(&mut vector);
            });
        })
    }
}
