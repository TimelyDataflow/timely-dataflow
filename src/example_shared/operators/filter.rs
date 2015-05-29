use communication::*;
use communication::pact::Pipeline;

use example_shared::*;
use example_shared::operators::unary::UnaryStreamExt;

use drain::DrainExt;

pub trait FilterExt<D: Data> {
    fn filter<L: Fn(&D)->bool+'static>(&self, logic: L) -> Self;
}

impl<G: GraphBuilder, D: Data> FilterExt<D> for Stream<G, D> {
    fn filter<L: Fn(&D)->bool+'static>(&self, logic: L) -> Stream<G, D> {
        self.unary_stream(Pipeline, format!("Filter"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                output.give_at(&time, data.drain_temp().filter(|x| logic(x)));
            }
        })
    }
}
