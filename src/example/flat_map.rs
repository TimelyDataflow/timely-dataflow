use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

pub trait FlatMapExt<'a, G: Graph+'a, D1: Data> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(&mut self, logic: L) -> Stream<'a, G, D2>;
}

impl<'a, G: Graph+'a, D1: Data> FlatMapExt<'a, G, D1> for Stream<'a, G, D1> {
    fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D1)->I+'static>(&mut self, logic: L) -> Stream<'a, G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().flat_map(|x| logic(x)));
            }
        })
    }
}
