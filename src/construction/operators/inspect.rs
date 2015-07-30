use communication::Data;
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::UnaryStreamExt;

pub trait InspectExt<G: GraphBuilder, D: Data> {
    fn inspect<F: FnMut(&D)+'static>(&self, func: F) -> Self;
    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Self;
}

impl<G: GraphBuilder, D: Data> InspectExt<G, D> for Stream<G, D> {
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
