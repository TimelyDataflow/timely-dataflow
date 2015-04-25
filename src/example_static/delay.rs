use std::hash::Hash;
use std::collections::HashMap;

use communication::*;
use communication::pact::Pipeline;

use example_static::stream::ActiveStream;
use example_static::unary::*;
use example_static::builder::*;

pub trait DelayExt<G: GraphBuilder, D: Data> { fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(self, F) -> Self; }

impl<G: GraphBuilder, D: Data> DelayExt<G, D> for ActiveStream<G, D>
where G::Timestamp: Hash {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(self, func: F) -> ActiveStream<G, D> {
        let _threshold = 256; // TODO : make more "streaming" by flushing
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, format!("Delay"), vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                for datum in data.drain() {
                    let mut new_time = func(&datum, &time);
                    if !(new_time >= time) {
                        new_time = time;
                    }

                    let mut list = elements.entry(new_time.clone()).or_insert_with(|| {
                        notificator.notify_at(&new_time);
                        Vec::new()
                    });
                    list.push(datum);
                }
            }
            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.give_at(&time, data.drain());
                }
            }
        })
    }
}
