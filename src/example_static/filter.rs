use communication::*;
use communication::pact::Pipeline;

use example_static::builder::GraphBuilder;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryExt;

pub trait FilterExt<D: Data> {
    fn filter<L: Fn(&D)->bool+'static>(self, logic: L) -> Self;
}

impl<G: GraphBuilder, D: Data> FilterExt<D> for ActiveStream<G, D> {
    fn filter<L: Fn(&D)->bool+'static>(self, logic: L) -> ActiveStream<G, D> {
        self.unary(Pipeline, format!("Filter"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().filter(|x| logic(x)));
            }
        })
    }
}
