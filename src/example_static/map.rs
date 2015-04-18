// use progress::Graph;
use communication::*;
use communication::pact::Pipeline;

use example_static::builder::*;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryExt;

pub trait MapExt<G: GraphBuilder, D1: Data> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(self, logic: L) -> ActiveStream<G, D2>;
}

impl<G: GraphBuilder, D1: Data> MapExt<G, D1> for ActiveStream<G, D1> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(self, logic: L) -> ActiveStream<G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().map(|x| logic(x)));
            }
        })
    }
}
