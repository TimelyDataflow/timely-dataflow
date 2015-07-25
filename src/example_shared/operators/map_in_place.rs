use communicator::Data;
use communicator::pact::Pipeline;
use observer::Extensions;

use example_shared::*;
use example_shared::operators::unary::UnaryStreamExt;

pub trait MapInPlaceExt<G: GraphBuilder, D: Data> {
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data> MapInPlaceExt<G, D> for Stream<G, D> {
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<G, D> {
        self.unary_stream(Pipeline, format!("MapInPlace"), move |input, output| {
            while let Some((time, data)) = input.pull() {
                for datum in data.take().iter_mut() { logic(datum); }
                output.give_message_at(time, data);
            }
        })
    }
}
