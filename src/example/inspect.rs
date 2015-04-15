use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

pub trait InspectExt<G: Graph, D: Data> {
    fn inspect<F: FnMut(&D)+'static>(&mut self, func: F) -> Self;
}

impl<G: Graph, D: Data> InspectExt<G, D> for Stream<G, D> {
    fn inspect<F: FnMut(&D)+'static>(&mut self, mut func: F) -> Stream<G, D> {
        self.unary(Pipeline, format!("Inspect"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for datum in data {
                    func(&datum);
                    session.give(datum);
                }
            }
        })
    }
}
