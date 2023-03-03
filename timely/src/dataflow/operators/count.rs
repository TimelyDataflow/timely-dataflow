//! Counts the number of records at each time.
use std::collections::HashMap;

use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{OwnedStream, Scope, StreamLike};
use crate::dataflow::operators::generic::operator::Operator;

/// Accumulates records within a timestamp.
pub trait Accumulate<G: Scope, D: Data>: Sized {
    /// Accumulates records within a timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Accumulate, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let captured = timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .accumulate(0, |sum, data| { for &x in data.iter() { *sum += x; } })
    ///            .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted, vec![(0, vec![45])]);
    /// ```
    fn accumulate<A: Data>(self, default: A, logic: impl Fn(&mut A, &mut Vec<D>)+'static) -> OwnedStream<G, Vec<A>>;
    /// Counts the number of records observed at each time.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Accumulate, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let captured = timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .count()
    ///            .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted, vec![(0, vec![10])]);
    /// ```
    fn count(self) -> OwnedStream<G, Vec<usize>> {
        self.accumulate(0, |sum, data| *sum += data.len())
    }
}

impl<G: Scope, D: Data, S: StreamLike<G, Vec<D>>> Accumulate<G, D> for S {
    fn accumulate<A: Data>(self, default: A, logic: impl Fn(&mut A, &mut Vec<D>)+'static) -> OwnedStream<G, Vec<A>> {

        let mut accums = HashMap::new();
        self.unary_notify(Pipeline, "Accumulate", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                logic(accums.entry(time.time().clone()).or_insert_with(|| default.clone()), data);
                notificator.notify_at(time.retain());
            });

            notificator.for_each(|time,_,_| {
                if let Some(accum) = accums.remove(&time) {
                    output.session(&time).give(accum);
                }
            });
        })
    }
}
