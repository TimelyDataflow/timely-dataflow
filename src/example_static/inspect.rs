use communication::*;
use communication::pact::Pipeline;

use example_static::builder::GraphBuilder;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryStreamExt;

pub trait InspectExt<D: Data> {
    fn inspect<F: FnMut(&D)+'static>(self, func: F) -> Self;
}

impl<G: GraphBuilder, D: Data> InspectExt<D> for ActiveStream<G, D> {
    fn inspect<F: FnMut(&D)+'static>(self, mut func: F) -> ActiveStream<G, D> {
        self.unary_stream(Pipeline, format!("Inspect"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                let mut session = output.session(&time);
                for datum in data.drain() {
                    func(&datum);
                    session.give(datum);
                }
            }
        })
    }
}


pub trait InspectBatchExt<G: GraphBuilder, D: Data> {
    fn inspect_batch<F: FnMut(&G::Timestamp, &Vec<D>)+'static>(self, mut func: F) -> Self;
}

impl<G: GraphBuilder, D: Data> InspectBatchExt<G, D> for ActiveStream<G, D> {
    fn inspect_batch<F: FnMut(&G::Timestamp, &Vec<D>)+'static>(self, mut func: F) -> ActiveStream<G, D> {
        self.unary_stream(Pipeline, format!("Inspect"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                func(&time, data);
                output.give_at(&time, data.drain());
            }
        })
    }
}
