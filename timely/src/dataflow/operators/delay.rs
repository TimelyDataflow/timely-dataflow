//! Operators acting on timestamps to logically delay records

use crate::Data;
use crate::order::TotalOrder;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::core::{Delay as DelayCore};

/// Methods to advance the timestamps of records or batches of records.
pub trait Delay<G: Scope, D: Data> {

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
    fn delay<L: FnMut(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: L) -> Self;

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
    fn delay_total<L: FnMut(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: L) -> Self
    where G::Timestamp: TotalOrder;

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
    fn delay_batch<L: FnMut(&G::Timestamp)->G::Timestamp+'static>(&self, func: L) -> Self;
}

impl<G: Scope, D: Data> Delay<G, D> for Stream<G, D> {
    fn delay<L: FnMut(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: L) -> Self {
        DelayCore::delay(self, func)
    }

    fn delay_total<L: FnMut(&D, &G::Timestamp)->G::Timestamp+'static>(&self, func: L) -> Self
    where G::Timestamp: TotalOrder
    {
        Delay::delay(self, func)
    }

    fn delay_batch<L: FnMut(&G::Timestamp)->G::Timestamp+'static>(&self, func: L) -> Self {
        DelayCore::delay_batch(self, func)
    }
}
