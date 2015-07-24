use std::hash::Hash;
use std::collections::HashMap;

use communication::*;
use communication::pact::Pipeline;

use example_shared::*;
use example_shared::operators::unary::UnaryNotifyExt;

use drain::DrainExt;

pub trait DelayExt<G: GraphBuilder, D: Data> {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
    fn delay_batch<F: Fn(&G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
}

impl<G: GraphBuilder, D: Data> DelayExt<G, D> for Stream<G, D>
where G::Timestamp: Hash {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: F) -> Stream<G, D> {

        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, format!("Delay"), vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                for datum in data.drain_temp() {
                    let mut new_time = func(&datum, &time);
                    if !(new_time >= time) {
                        new_time = time;
                    }

                    elements.entry(new_time.clone())
                            .or_insert_with(|| { notificator.notify_at(&new_time); Vec::new() })
                            .push(datum);
                }
            }
            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.give_at(&time, data.drain_temp());
                }
            }
        })
    }

    fn delay_batch<F: Fn(&G::Timestamp)->G::Timestamp+'static>(&self, func: F) -> Stream<G, D> {

        let mut stash = Vec::new();
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, format!("Delay"), vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                let mut new_time = func(&time);
                if !(new_time >= time) {
                    new_time = time;
                }

                let spare = if let Some(vec) = stash.pop() { vec } else { Vec::with_capacity(2048) };
                let data = ::std::mem::replace(data, spare);

                elements.entry(new_time.clone())
                        .or_insert_with(|| { notificator.notify_at(&new_time); Vec::new() })
                        .push(data);
            }

            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut datas) = elements.remove(&time) {
                    for mut data in datas.drain_temp() {
                        output.give_vector_at(&time, &mut data);
                        stash.push(data);
                    }
                }
            }
        })
    }
}
