use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

pub trait FlatMapExt<'a, 'b: 'a, G: Graph+'b, D1: Data> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(&mut self, logic: L) -> Stream<'a, 'b, G, D2>;
}

impl<'a, 'b: 'a, G: Graph+'b, D1: Data> FlatMapExt<'a, 'b, G, D1> for Stream<'a, 'b, G, D1> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(&mut self, logic: L) -> Stream<'a, 'b, G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().flat_map(|x| logic(x)));
            }
        })
    }
}
