// use progress::Graph;
use communication::*;
use communication::pact::Pipeline;

use example_static::builder::*;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryStreamExt;

use drain::DrainExt;

pub trait MapExt<G: GraphBuilder, D1: Data> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(self, logic: L) -> ActiveStream<G, D2>;
}

impl<G: GraphBuilder, D1: Data> MapExt<G, D1> for ActiveStream<G, D1> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(self, logic: L) -> ActiveStream<G, D2> {
        self.unary_stream(Pipeline, format!("Map"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                output.give_at(&time, data.drain_temp().map(|x| logic(x)));
            }
        })
    }
}
