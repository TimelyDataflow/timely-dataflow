//! General purpose intra-timestamp aggregation
use std::hash::Hash;
use std::collections::HashMap;

use crate::{Data, ExchangeData};
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::channels::pact::Exchange;

/// Generic intra-timestamp aggregation
///
/// Extension method supporting aggregation of keyed data within timestamp.
/// For inter-timestamp aggregation, consider `StateMachine`.
pub trait Aggregate<S: Scope, K: ExchangeData+Hash, V: ExchangeData> {
    /// Aggregates data of the form `(key, val)`, using user-supplied logic.
    ///
    /// The `aggregate` method is implemented for streams of `(K, V)` data,
    /// and takes functions `fold`, `emit`, and `hash`; used to combine new `V`
    /// data with existing `D` state, to produce `I` output from `D` state, and
    /// to route `K` keys, respectively.
    ///
    /// Aggregation happens within each time, and results are produced once the
    /// time is complete.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::operators::aggregation::Aggregate;
    ///
    /// timely::example(|scope| {
    ///
    ///     (0..10).to_stream(scope)
    ///         .map(|x| (x % 2, x))
    ///         .aggregate(
    ///             i32::default,
    ///             |_key, val, agg| { *agg += val; },
    ///             |key, agg| [(key, agg)],
    ///             |key| *key as u64
    ///         )
    ///         .inspect(|x| assert!(*x == (0, 20) || *x == (1, 25)));
    /// });
    /// ```
    ///
    /// By changing the type of the aggregate value, one can accumulate into different types.
    /// Here we accumulate the data into a `Vec<i32>` and report its length (which we could
    /// obviously do more efficiently; imagine we were doing a hash instead).
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::operators::aggregation::Aggregate;
    ///
    /// timely::example(|scope| {
    ///
    ///     (0..10).to_stream(scope)
    ///         .map(|x| (x % 2, x))
    ///         .aggregate(
    ///             Vec::default,
    ///             |_key, val, agg| { agg.push(val); },
    ///             |key, agg| [(key, agg.len())],
    ///             |key| *key as u64
    ///         )
    ///         .inspect(|x| assert!(*x == (0, 5) || *x == (1, 5)));
    /// });
    /// ```
    fn aggregate<I: IntoIterator, D: 'static, M: Fn() -> D+ 'static, F: Fn(&K, V, &mut D)+'static, E: Fn(K, D)->I+'static, H: Fn(&K)->u64+'static>(
        &self,
        make_default: M,
        fold: F,
        emit: E,
        hash: H) -> Stream<S, I::Item>
    where S::Timestamp: Eq,
          I::Item: Data;
}

impl<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData> Aggregate<S, K, V> for Stream<S, (K, V)> {

    fn aggregate<I: IntoIterator, D: 'static, M: Fn() -> D + 'static, F: Fn(&K, V, &mut D)+'static, E: Fn(K, D)->I+'static, H: Fn(&K)->u64+'static>(
        &self,
        make_default: M,
        fold: F,
        emit: E,
        hash: H) -> Stream<S, I::Item>
    where S::Timestamp: Eq,
          I::Item: Data
    {

        let mut aggregates = HashMap::new();
        let mut vector = Vec::new();
        self.unary_notify(Exchange::new(move |&(ref k, _)| hash(k)), "Aggregate", vec![], move |input, output, notificator| {

            // read each input, fold into aggregates
            input.for_each(|time, data| {
                data.swap(&mut vector);
                let agg_time = aggregates.entry(time.time().clone()).or_insert_with(HashMap::new);
                for (key, val) in vector.drain(..) {
                    let agg = agg_time.entry(key.clone()).or_insert_with(&make_default);
                    fold(&key, val, agg);
                }
                notificator.notify_at(time.retain());
            });

            // pop completed aggregates, send along whatever
            notificator.for_each(|time,_,_| {
                if let Some(aggs) = aggregates.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (key, agg) in aggs {
                        session.give_iterator(emit(key, agg).into_iter());
                    }
                }
            });
        })

    }
}
