//! Operators acting on timestamps to logically delay records

use std::collections::HashMap;
use timely_container::{Container, PushContainer, PushInto};

use crate::order::{PartialOrder, TotalOrder};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::operators::generic::operator::Operator;

/// Methods to advance the timestamps of records or batches of records.
pub trait Delay<G: Scope, C: Container> {

    /// Advances the timestamp of records using a supplied function.
    ///
    /// The function *must* advance the timestamp; the operator will test that the
    /// new timestamp is greater or equal to the old timestamp, and will assert if
    /// it is not.
    ///
    /// # Examples
    ///
    /// The following example takes the sequence `0..10` at time `0`
    /// and delays each element `i` to time `i`.
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Operator};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .delay(|data, time| *data)
    ///            .sink(Pipeline, "example", |input| {
    ///                input.for_each(|time, data| {
    ///                    println!("data at time: {:?}", time);
    ///                });
    ///            });
    /// });
    /// ```
    fn delay<L>(&self, func: L) -> Self
    where
        for<'a> L: FnMut(&C::Item<'a>, &G::Timestamp)->G::Timestamp+'static;

    /// Advances the timestamp of records using a supplied function.
    ///
    /// This method is a specialization of `delay` for when the timestamp is totally
    /// ordered. In this case, we can use a priority queue rather than an unsorted
    /// list to manage the potentially available timestamps.
    ///
    /// # Examples
    ///
    /// The following example takes the sequence `0..10` at time `0`
    /// and delays each element `i` to time `i`.
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Operator};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .delay(|data, time| *data)
    ///            .sink(Pipeline, "example", |input| {
    ///                input.for_each(|time, data| {
    ///                    println!("data at time: {:?}", time);
    ///                });
    ///            });
    /// });
    /// ```
    fn delay_total<L>(&self, func: L) -> Self
    where
        G::Timestamp: TotalOrder,
        for<'a> L: FnMut(&C::Item<'a>, &G::Timestamp)->G::Timestamp+'static;

    /// Advances the timestamp of batches of records using a supplied function.
    ///
    /// The operator will test that the new timestamp is greater or equal to the
    /// old timestamp, and will assert if it is not. The batch version does not
    /// consult the data, and may only view the timestamp itself.
    ///
    /// # Examples
    ///
    /// The following example takes the sequence `0..10` at time `0`
    /// and delays each batch (there is just one) to time `1`.
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Operator};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .delay_batch(|time| time + 1)
    ///            .sink(Pipeline, "example", |input| {
    ///                input.for_each(|time, data| {
    ///                    println!("data at time: {:?}", time);
    ///                });
    ///            });
    /// });
    /// ```
    fn delay_batch<L>(&self, func: L) -> Self
    where
        L: FnMut(&G::Timestamp)->G::Timestamp+'static;
}

impl<G: Scope, C: PushContainer> Delay<G, C> for StreamCore<G, C>
where
    for<'a> C::Item<'a>: PushInto<C>,
{
    fn delay<L>(&self, mut func: L) -> Self
    where
        for<'a> L: FnMut(&C::Item<'a>, &G::Timestamp)->G::Timestamp+'static
    {
        let mut elements = HashMap::new();
        let mut vector = C::default();
        self.unary_notify(Pipeline, "Delay", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                for datum in vector.drain() {
                    let new_time = func(&datum, &time);
                    assert!(time.time().less_equal(&new_time));
                    elements.entry(new_time.clone())
                            .or_insert_with(|| { notificator.notify_at(time.delayed(&new_time)); C::default() })
                            .push(datum);
                }
            });

            // for each available notification, send corresponding set
            notificator.for_each(|time,_,_| {
                if let Some(mut data) = elements.remove(&time) {
                    output.session(&time).give_iterator(data.drain());
                }
            });
        })
    }

    fn delay_total<L>(&self, func: L) -> Self
    where
        G::Timestamp: TotalOrder,
        for<'a> L: FnMut(&C::Item<'a>, &G::Timestamp)->G::Timestamp+'static
    {
        self.delay(func)
    }

    fn delay_batch<L: FnMut(&G::Timestamp)->G::Timestamp+'static>(&self, mut func: L) -> Self {
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Delay", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                let new_time = func(&time);
                assert!(time.time().less_equal(&new_time));
                elements.entry(new_time.clone())
                        .or_insert_with(|| { notificator.notify_at(time.delayed(&new_time)); Vec::new() })
                        .push(data.replace(C::default()));
            });

            // for each available notification, send corresponding set
            notificator.for_each(|time,_,_| {
                if let Some(mut datas) = elements.remove(&time) {
                    for mut data in datas.drain(..) {
                        output.session(&time).give_container(&mut data);
                    }
                }
            });
        })
    }
}
