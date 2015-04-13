use progress::Graph;
use communication::Pullable;
use communication::channels::Data;
use communication::exchange::Pipeline;
use communication::observer::ObserverSessionExt;
use example::stream::Stream;
use example::unary::UnaryExt;

// a trait enabling the "select" method
pub trait SelectExt<G: Graph, D1: Data, D2: Data> {
    fn select<L: Fn(D1)->D2+'static>(&mut self, logic: L) -> Stream<G, D2>;
}

// implement the extension trait for Streams
impl<G: Graph, D1: Data, D2: Data> SelectExt<G, D1, D2> for Stream<G, D1> {
    fn select<L: Fn(D1)->D2+'static>(&mut self, logic: L) -> Stream<G, D2> {
        self.unary(Pipeline, format!("Select"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for datum in data {
                    session.give(logic(datum));
                }
            }
        })
    }
}
