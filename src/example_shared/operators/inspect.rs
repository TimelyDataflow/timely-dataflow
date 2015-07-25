use communication::Data;
use communication::pact::Pipeline;

use example_shared::*;
use example_shared::operators::unary::UnaryStreamExt;

use communication::observer::Extensions;

pub trait InspectExt<D: Data> {
    fn inspect<F: FnMut(&D)+'static>(&self, func: F) -> Self;
}

// TODO : could use look() rather than take()
impl<G: GraphBuilder, D: Data> InspectExt<D> for Stream<G, D> {
    fn inspect<F: FnMut(&D)+'static>(&self, mut func: F) -> Stream<G, D> {
        self.unary_stream(Pipeline, format!("Inspect"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                for datum in data.look().iter() { func(datum); }
                output.give_message_at(time, data);
            }
        })
    }
}


pub trait InspectBatchExt<G: GraphBuilder, D: Data> {
    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Self;
}

// TODO : could use look() rather than take()
impl<G: GraphBuilder, D: Data> InspectBatchExt<G, D> for Stream<G, D> {
    fn inspect_batch<F: FnMut(&G::Timestamp, &[D])+'static>(&self, mut func: F) -> Stream<G, D> {
        self.unary_stream(Pipeline, format!("Inspect"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                func(&time, &data.look()[..]);
                output.give_message_at(time, data);
            }
        })
    }
}
