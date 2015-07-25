use std::hash::Hash;
use std::collections::HashMap;

use communication::{Data, Pullable, Message};
use communication::pact::Pipeline;
use communication::observer::Extensions;

use construction::{Stream, GraphBuilder};
use construction::operators::unary::UnaryNotifyExt;

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
                for datum in data.take().drain_temp() {
                    let new_time = func(&datum, time);
                    if !(&new_time >= time) {
                        println!("error; new_time : {} >!= {} : time", new_time, time);
                    }
                    assert!(&new_time >= time);
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
                let new_time = func(time);
                assert!(&new_time >= time);
                let spare = stash.pop().unwrap_or_else(|| Vec::with_capacity(4096));
                let data = ::std::mem::replace(data.take(), spare);

                elements.entry(new_time.clone())
                        .or_insert_with(|| { notificator.notify_at(&new_time); Vec::new() })
                        .push(data);
            }

            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut datas) = elements.remove(&time) {
                    for data in datas.drain_temp() {
                        let mut message = Message::Typed(data);
                        output.give_message_at(&time, &mut message);
                        if let Message::Typed(data) = message {
                            stash.push(data);
                        }
                    }
                }
            }
        })
    }
}
