use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

pub trait InspectExt<D: Data> {
    fn inspect<F: FnMut(&D)+'static>(&mut self, func: F) -> Self;
}

impl<'a, G: Graph+'a, D: Data> InspectExt<D> for Stream<'a, G, D> {
    fn inspect<F: FnMut(&D)+'static>(&mut self, mut func: F) -> Stream<'a, G, D> {
        self.unary(Pipeline, format!("Inspect"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for datum in data {
                    func(&datum);
                    session.give(datum);
                }
            }
        })
    }
}


pub trait InspectBatchExt<G: Graph, D: Data> {
    fn inspect_batch<F: FnMut(&G::Timestamp, &Vec<D>)+'static>(&mut self, mut func: F) -> Self;
}

impl<'a, G: Graph+'a, D: Data> InspectBatchExt<G, D> for Stream<'a, G, D> {
    fn inspect_batch<F: FnMut(&G::Timestamp, &Vec<D>)+'static>(&mut self, mut func: F) -> Stream<'a, G, D> {
        self.unary(Pipeline, format!("Inspect"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                func(&time, &data);
                handle.output.give_at(&time, data.into_iter());
            }
        })
    }
}
