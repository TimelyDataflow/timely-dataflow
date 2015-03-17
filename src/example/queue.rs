use progress::Graph;
use communication::{Communicator, Pullable};
use communication::channels::Data;
use communication::exchange::exchange_with;
use communication::observer::ObserverSessionExt;
use example::stream::Stream;
use example::unary::UnaryExt;

use columnar::Columnar;

pub trait QueueExtensionTrait { fn queue(&mut self) -> Self; }

impl<G: Graph, D: Data+Columnar, C: Communicator> QueueExtensionTrait for Stream<G, D, C> {
    fn queue(&mut self) -> Stream<G, D, C> {
        let (sender, receiver) = { exchange_with(&mut (*self.allocator.borrow_mut()), |_| 0) };
        let mut temp = Vec::new();
        self.unary(sender, receiver, move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                temp.push((time, data));
            }

            while let Some((time, data)) = temp.pop() {
                let mut session = handle.output.session(&time);
                for datum in &data { session.push(datum); }
            }
        })
    }
}
