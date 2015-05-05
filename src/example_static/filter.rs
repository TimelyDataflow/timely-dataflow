use communication::*;
use communication::pact::Pipeline;

use example_static::builder::GraphBuilder;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryStreamExt;

pub trait FilterExt<D: Data> {
    fn filter<L: Fn(&D)->bool+'static>(self, logic: L) -> Self;
}

impl<G: GraphBuilder, D: Data> FilterExt<D> for ActiveStream<G, D> {
    fn filter<L: Fn(&D)->bool+'static>(self, logic: L) -> ActiveStream<G, D> {
        self.unary_stream(Pipeline, format!("Filter"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                output.give_at(&time, data.drain(..).filter(|x| logic(x)));
            }
        })
    }
}
