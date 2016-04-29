//! Operators acting on timestamps to logically delay records

use std::hash::Hash;
use std::collections::HashMap;
use std::ops::DerefMut;

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::channels::Content;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

/// Methods to advance the timestamps of records or batches of records.
pub trait Delay<G: Scope, D: Data> {
    /// Advances the timestamp of records using a supplied function.
    ///
    /// The function *must* advance the timestamp; the operator will test that the 
    /// new timestamp is greater or equal to the old timestamp, and will assert if 
    /// it is not.
    ///
    /// #Examples
    ///
    /// The following example takes the sequence `0..10` at time `RootTimestamp(0)`
    /// and delays each element `i` to time `RootTimestamp(i)`.
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Unary};
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .delay(|data, time| RootTimestamp::new(*data))
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                while let Some((time, data)) = input.next() {
    ///                    println!("data at time: {:?}", time);
    ///                    output.session(&time).give_content(data);
    ///                }
    ///            });
    /// });
    /// ```    
    fn delay<F: Fn(&D, &G::Timestamp)->G::Timestamp+'static>(&self, F) -> Self;
    /// Advances the timestamp of batches of records using a supplied function.
    ///
    /// The function *must* advance the timestamp; the operator will test that the 
    /// new timestamp is greater or equal to the old timestamp, and will assert if 
    /// it is not. The batch version does not consult the data, and may only view
    /// the timestamp itself.
    ///
    /// #Examples
    ///
    /// The following example takes the sequence `0..10` at time `RootTimestamp(0)`
    /// and delays each batch (there is just one) to time `RootTimestamp(1)`.
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Unary};
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .delay_batch(|time| RootTimestamp::new(time.inner + 1))
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                while let Some((time, data)) = input.next() {
    ///                    println!("data at time: {:?}", time);
    ///                    output.session(&time).give_content(data);
    ///                }
    ///            });
    /// });
    /// ```        
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
                    elements.entry(new_time)
                            .or_insert_with(|| { notificator.notify_at(time.delayed(&new_time)); Vec::new() })
                            .push(datum);
                }
            }
            // for each available notification, send corresponding set
            for (time, _count) in notificator {
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
                let spare = stash.pop().unwrap_or_else(Vec::new);
                let data = ::std::mem::replace(data.deref_mut(), spare);

                elements.entry(new_time)
                        .or_insert_with(|| { notificator.notify_at(time.delayed(&new_time)); Vec::new() })
                        .push(data);
            }

            // for each available notification, send corresponding set
            for (time, _count) in notificator {
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
