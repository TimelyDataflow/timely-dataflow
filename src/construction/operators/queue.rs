use std::collections::HashMap;
use std::hash::Hash;

use communication::Data;
use communication::pact::Pipeline;
use serialization::Serializable;
use communication::observer::Extensions;

use construction::{Stream, GraphBuilder};
use construction::operators::unary::UnaryNotifyExt;

use drain::DrainExt;

pub trait QueueExt {
    fn queue(&self) -> Self;
}

impl<G: GraphBuilder, D: Data+Serializable> QueueExt for Stream<G, D>
where G::Timestamp: Hash {

    fn queue(&self) -> Stream<G, D> {
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Queue", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                notificator.notify_at(time);
                let set = elements.entry((*time).clone()).or_insert(Vec::new());
                for datum in data.drain_temp() { set.push(datum); }
            }

            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.give_at(&time, data.drain_temp());
                }
            }
        })
    }
}
