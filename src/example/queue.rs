use progress::Graph;
use communication::*;
use communication::pact::Pipeline;
use example::stream::Stream;
use example::unary::UnaryExt;

use columnar::Columnar;

pub trait QueueExtensionTrait { fn queue(&mut self) -> Self; }

impl<'a, G: Graph+'a, D: Data+Columnar> QueueExtensionTrait for Stream<'a, G, D> {
    fn queue(&mut self) -> Stream<'a, G, D> {
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
