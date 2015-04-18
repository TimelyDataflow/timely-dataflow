use communication::*;
use communication::pact::Pipeline;

use example_static::builder::GraphBuilder;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryExt;

pub trait FlatMapExt<'a, G: GraphBuilder+'a, D1: Data> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(self, logic: L) -> ActiveStream<'a, G, D2>;
}

impl<'a, G: GraphBuilder+'a, D1: Data> FlatMapExt<'a, G, D1> for ActiveStream<'a, G, D1> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(self, logic: L) -> ActiveStream<'a, G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().flat_map(|x| logic(x)));
            }
        })
    }
}
