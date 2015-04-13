use progress::Graph;
use communication::Pullable;
use communication::channels::Data;
use communication::exchange::Pipeline;
use communication::observer::ObserverSessionExt;
use example::stream::Stream;
use example::unary::UnaryExt;

use columnar::Columnar;

pub trait QueueExtensionTrait { fn queue(&mut self) -> Self; }

impl<G: Graph, D: Data+Columnar> QueueExtensionTrait for Stream<G, D> {
    fn queue(&mut self) -> Stream<G, D> {
        let mut temp = Vec::new();
        self.unary(Pipeline, format!("Queue"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                temp.push((time, data));
            }

            while let Some((time, data)) = temp.pop() {
                let mut session = handle.output.session(&time);
                for datum in data { session.give(datum); }
            }
        })
    }
}
