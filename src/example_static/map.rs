// use progress::Graph;
use communication::*;
use communication::pact::Pipeline;

use example_static::builder::*;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryExt;

pub trait MapExt<'a, G: GraphBuilder+'a, D1: Data> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(self, logic: L) -> ActiveStream<'a, G, D2>;
}

impl<'a, G: GraphBuilder+'a, D1: Data> MapExt<'a, G, D1> for ActiveStream<'a, G, D1> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(self, logic: L) -> ActiveStream<'a, G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().map(|x| logic(x)));
            }
        })
    }
}
