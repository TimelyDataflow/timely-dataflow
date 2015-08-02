use std::hash::Hash;
use std::collections::HashMap;
use std::ops::DerefMut;

use communication::{Data, Pullable, Message};
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::Extension;

use drain::DrainExt;

pub trait Delay<G: GraphBuilder, D: Data> {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
    fn delay_batch<F: Fn(&G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
}

impl<G: GraphBuilder, D: Data> Delay<G, D> for Stream<G, D>
where G::Timestamp: Hash {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: F) -> Stream<G, D> {
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Delay", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                for datum in data.drain_temp() {
                    let new_time = func(&datum, time);
                    assert!(&new_time >= time);
                    elements.entry(new_time.clone())
                            .or_insert_with(|| { notificator.notify_at(&new_time); Vec::new() })
                            .push(datum);
                }
            }
            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.session(&time).give_iterator(data.drain_temp());
                }
            }
        })
    }

    fn delay_batch<F: Fn(&G::Timestamp)->G::Timestamp+'static>(&self, func: F) -> Stream<G, D> {
        let mut stash = Vec::new();
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Delay", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                let new_time = func(time);
                assert!(&new_time >= time);
                let spare = stash.pop().unwrap_or_else(|| Vec::new());
                let data = ::std::mem::replace(data.deref_mut(), spare);

                elements.entry(new_time.clone())
                        .or_insert_with(|| { notificator.notify_at(&new_time); Vec::new() })
                        .push(data);
            }

            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut datas) = elements.remove(&time) {
                    for mut data in datas.drain_temp() {
                        let mut message = Message::from_typed(&mut data);
                        output.session(&time).give_message(&mut message);
                        let buffer = message.into_typed();
                        if buffer.capacity() == Message::<D>::default_length() { stash.push(buffer); }
                    }
                }
            }
        })
    }
}
