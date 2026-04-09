//! Counts the number of records at each time.
use std::collections::HashMap;

use crate::dataflow::channels::pact::Pipeline;
use crate::progress::Timestamp;
use crate::dataflow::StreamVec;
use crate::dataflow::operators::generic::operator::Operator;

/// Accumulates records within a timestamp.
pub trait Accumulate<T: Timestamp, D: 'static> : Sized {
    /// Accumulates records within a timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::vec::count::Accumulate;
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
    fn accumulate<A: Clone+'static>(self, default: A, logic: impl Fn(&mut A, &mut Vec<D>)+'static) -> StreamVec<T, A>;
    /// Counts the number of records observed at each time.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::vec::count::Accumulate;
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
    fn count(self) -> StreamVec<T, usize> {
        self.accumulate(0, |sum, data| *sum += data.len())
    }
}

impl<T: Timestamp + ::std::hash::Hash, D: 'static> Accumulate<T, D> for StreamVec<T, D> {
    fn accumulate<A: Clone+'static>(self, default: A, logic: impl Fn(&mut A, &mut Vec<D>)+'static) -> StreamVec<T, A> {

        let mut accums = HashMap::new();
        self.unary_notify(Pipeline, "Accumulate", vec![], move |input, output, notificator| {
            input.for_each_time(|time, data| {
                for data in data {
                    logic(accums.entry(time.time().clone()).or_insert_with(|| default.clone()), data);
                }
                notificator.notify_at(time.retain(output.output_index()));
            });

            notificator.for_each(|time,_,_| {
                if let Some(accum) = accums.remove(&time) {
                    output.session(&time).give(accum);
                }
            });
        })
    }
}
