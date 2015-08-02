use communication::Data;
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::Extension;

pub trait MapInPlaceExt<G: GraphBuilder, D: Data> {
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data> MapInPlaceExt<G, D> for Stream<G, D> {
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<G, D> {
        self.unary_stream(Pipeline, "MapInPlace", move |input, output| {
            while let Some((time, data)) = input.pull() {
                for datum in data.iter_mut() { logic(datum); }
                output.session(time).give_message(data);
            }
        })
    }
}
