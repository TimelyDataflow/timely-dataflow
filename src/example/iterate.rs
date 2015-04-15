use progress::Graph;
use communication::Pullable;
use communication::channels::Data;
use communication::exchange::Pipeline;
use communication::observer::ObserverSessionExt;
use example::stream::Stream;
use example::unary::UnaryExt;

pub trait IterateExt<G: Graph, D1: Data, D2: Data> {
    fn iterate<I: Iterator<Item=D2>, L: Fn(Vec<D1>)->I+'static>(&mut self, name: String, logic: L) -> Stream<G, D2>;
}

impl<G: Graph, D1: Data, D2: Data> IterateExt<G, D1, D2> for Stream<G, D1> {
    fn iterate<I: Iterator<Item=D2>, L: Fn(Vec<D1>)->I+'static>(&mut self, nome: String, logic: L) -> Stream<G, D2> {
        self.unary(Pipeline, name, move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, logic(data));
            }
        })
    }
}
