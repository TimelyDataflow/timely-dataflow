use std::collections::HashMap;
use std::hash::Hash;

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

pub trait Count<G: Scope> {
    /// Counts the number of records observed at each time.
    fn count(&self) -> Stream<G, usize>;
}

impl<G: Scope, D: Data> Count<G> for Stream<G, D>
where G::Timestamp: Hash {
    fn count(&self) -> Stream<G, usize> {

        let mut counts = HashMap::new();
        self.unary_notify(Pipeline, "Count", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                *counts.entry(time.time()).or_insert(0) += data.len();
                notificator.notify_at(time);
            }

            for (time, _count) in notificator {
                if let Some(count) = counts.remove(&time) {
                    output.session(&time).give(count);
                }
            }
        })
    }
}
