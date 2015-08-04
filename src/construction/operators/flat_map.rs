use ::Data;
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::Extension;

use drain::DrainExt;

pub trait FlatMapExt<G: GraphBuilder, D1: Data> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(&self, logic: L) -> Stream<G, D2>;
}

impl<G: GraphBuilder, D1: Data> FlatMapExt<G, D1> for Stream<G, D1> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(&self, logic: L) -> Stream<G, D2> {
        self.unary_stream(Pipeline, "FlatMap", move |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(time).give_iterator(data.drain_temp().flat_map(|x| logic(x)));
            }
        })
    }
}
