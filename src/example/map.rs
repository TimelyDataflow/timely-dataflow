use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

pub trait MapExt<'a, G: Graph+'a, D1: Data> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(&mut self, logic: L) -> Stream<'a, G, D2>;
}

impl<'a, G: Graph+'a, D1: Data> MapExt<'a, G, D1> for Stream<'a, G, D1> {
    fn map<D2: Data, L: Fn(D1)->D2+'static>(&mut self, logic: L) -> Stream<'a, G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().map(|x| logic(x)));
            }
        })
    }
}
