use std::collections::HashMap;
use std::hash::Hash;

use communication::*;
use communication::pact::Pipeline;

use example_shared::*;
use example_shared::operators::unary::UnaryNotifyExt;

use columnar::Columnar;
use drain::DrainExt;

pub trait QueueExt {
    fn queue(&self) -> Self;
}

impl<G: GraphBuilder, D: Data+Columnar> QueueExt for Stream<G, D>
where G::Timestamp: Hash {

    fn queue(&self) -> Stream<G, D> {
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, format!("Queue"), vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                let set = elements.entry(time).or_insert(Vec::new());
                for datum in data.drain_temp() { set.push(datum); }

                notificator.notify_at(&time);
            }

            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.give_at(&time, data.drain_temp());
                }
            }
        })
    }
}
