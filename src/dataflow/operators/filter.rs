use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

pub trait FilterExt<D: Data> {
    fn filter<L: Fn(&D)->bool+'static>(&self, logic: L) -> Self;
}

impl<G: Scope, D: Data> FilterExt<D> for Stream<G, D> {
    fn filter<L: Fn(&D)->bool+'static>(&self, logic: L) -> Stream<G, D> {
        self.unary_stream(Pipeline, "Filter", move |input, output| {
            while let Some((time, data)) = input.next() {
                data.retain(|x| logic(x));
                if data.len() > 0 {
                    output.session(time).give_content(data);
                }
            }
        })
    }
}
