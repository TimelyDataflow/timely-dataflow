use std::hash::Hash;
use std::collections::HashMap;
use std::ops::DerefMut;

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::channels::Content;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

pub trait Delay<G: Scope, D: Data> {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
    fn delay_batch<F: Fn(&G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
}

impl<G: Scope, D: Data> Delay<G, D> for Stream<G, D>
where G::Timestamp: Hash {
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: F) -> Stream<G, D> {
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Delay", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                for datum in data.drain(..) {
                    let new_time = func(&datum, &time);
                    assert!(new_time >= time.time());
                    elements.entry(new_time.clone())
                            .or_insert_with(|| { notificator.notify_at(time.delayed(&new_time)); Vec::new() })
                            .push(datum);
                }
            }
            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.session(&time).give_iterator(data.drain(..));
                }
            }
        })
    }

    fn delay_batch<F: Fn(&G::Timestamp)->G::Timestamp+'static>(&self, func: F) -> Stream<G, D> {
        let mut stash = Vec::new();
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Delay", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                let new_time = func(&time);
                assert!(new_time >= time.time());
                let spare = stash.pop().unwrap_or_else(|| Vec::new());
                let data = ::std::mem::replace(data.deref_mut(), spare);

                elements.entry(new_time.clone())
                        .or_insert_with(|| { notificator.notify_at(time.delayed(&new_time)); Vec::new() })
                        .push(data);
            }

            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {
                if let Some(mut datas) = elements.remove(&time) {
                    for mut data in datas.drain(..) {
                        let mut message = Content::from_typed(&mut data);
                        output.session(&time).give_content(&mut message);
                        let buffer = message.into_typed();
                        if buffer.capacity() == Content::<D>::default_length() { stash.push(buffer); }
                    }
                }
            }
        })
    }
}
