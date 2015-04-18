use communication::*;
use communication::pact::Pipeline;

use example_static::builder::GraphBuilder;
use example_static::stream::ActiveStream;
use example_static::unary::UnaryExt;

// a trait enabling the "select" method
pub trait FilterExt<D: Data> {
    fn filter<L: Fn(&D)->bool+'static>(self, logic: L) -> Self;
}

// implement the extension trait for Streams
impl<'a, G: GraphBuilder+'a, D: Data> FilterExt<D> for ActiveStream<'a, G, D> {
    fn filter<L: Fn(&D)->bool+'static>(self, logic: L) -> ActiveStream<'a, G, D> {
        // TODO : Would like the following (simplicity) but no works.
        // self.iterate(format!("Where"), move |x| x.into_iter().filter(|x| logic(x)))
        self.unary(Pipeline, format!("Where"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().filter(|x| logic(x)));
            }
        })
    }
}
