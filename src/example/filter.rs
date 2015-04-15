use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

// a trait enabling the "select" method
pub trait FilterExt<D: Data> {
    fn filter<L: Fn(&D)->bool+'static>(&mut self, logic: L) -> Self;
}

// implement the extension trait for Streams
impl<G: Graph, D: Data> FilterExt<D> for Stream<G, D> {
    fn filter<L: Fn(&D)->bool+'static>(&mut self, logic: L) -> Stream<G, D> {
        // TODO : Would like the following (simplicity) but no works.
        // self.iterate(format!("Where"), move |x| x.into_iter().filter(|x| logic(x)))
        self.unary(Pipeline, format!("Where"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().filter(|x| logic(x)));
            }
        })
    }
}
