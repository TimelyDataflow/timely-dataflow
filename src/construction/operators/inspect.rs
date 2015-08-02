//! Extension trait and implementation for observing and action on streamed data.

use communication::Data;
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::Extension;

/// Methods to inspect records and batches of records on a stream.
pub trait Inspect<G: GraphBuilder, D: Data> {
    /// Creates a dataflow operator that calls the supplied function on each record.
    fn inspect<F: FnMut(&D)+'static>(&self, func: F) -> Self;
    /// Creates a dataflow operator that calls the supplied function on each timestamp and batch of records.
    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Self;
}

impl<G: GraphBuilder, D: Data> Inspect<G, D> for Stream<G, D> {
    fn inspect<F: FnMut(&D)+'static>(&self, mut func: F) -> Stream<G, D> {
        self.unary_stream(Pipeline, "Inspect", move |input, output| {
            while let Some((time, data)) = input.pull() {
                for datum in data.iter() { func(datum); }
                output.session(time).give_message(data);
            }
        })
    }

    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Stream<G, D> {
        self.unary_stream(Pipeline, "InspectBatch", move |input, output| {
            while let Some((time, data)) = input.pull() {
                func(&time, &data[..]);
                output.session(time).give_message(data);
            }
        })
    }
}
