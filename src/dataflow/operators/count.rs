//! Counts the number of records at each time.
use std::collections::HashMap;
use std::hash::Hash;

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

use dataflow::channels::message::Content;

/// Accumulates records within a timestamp.
pub trait Accumulate<G: Scope, D: Data> {
    /// Accumulates records within a timestamp.
    ///
    /// #Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Accumulate, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// let captured = timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .accumulate(0, |sum, data| { for &x in data.iter() { *sum += x; } })
    ///            .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted, vec![(RootTimestamp::new(0), vec![45])]);
    /// ```            
    fn accumulate<A: Data, F: Fn(&mut A, &mut Content<D>)+'static>(&self, default: A, logic: F) -> Stream<G, A>;
    /// Counts the number of records observed at each time.
    ///
    /// #Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Accumulate, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// let captured = timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .count()
    ///            .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted, vec![(RootTimestamp::new(0), vec![10])]);
    /// ```        
    fn count(&self) -> Stream<G, usize> {
        self.accumulate(0, |sum, data| *sum += data.len())
    }
}

impl<G: Scope, D: Data> Accumulate<G, D> for Stream<G, D>
where G::Timestamp: Hash {
    fn accumulate<A: Data, F: Fn(&mut A, &mut Content<D>)+'static>(&self, default: A, logic: F) -> Stream<G, A> {

        let mut accums = HashMap::new();
        self.unary_notify(Pipeline, "Accumulate", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                logic(&mut accums.entry(time.time()).or_insert(default.clone()), data);
                notificator.notify_at(time);
            });

            notificator.for_each(|time,_,_| {
                if let Some(accum) = accums.remove(&time) {
                    output.session(&time).give(accum);
                }
            });
        })
    }
}
