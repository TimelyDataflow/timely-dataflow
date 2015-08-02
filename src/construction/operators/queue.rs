use std::collections::HashMap;
use std::hash::Hash;

use communication::Data;
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::Extension;

use drain::DrainExt;

pub trait Queue {
    fn queue(&self) -> Self;
}

impl<G: GraphBuilder, D: Data> Queue for Stream<G, D>
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
                    output.session(&time).give_iterator(data.drain_temp());
                }
            }
        })
    }
}
