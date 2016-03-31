/// General purpose intra-timestamp aggregation
use std::hash::Hash;
use std::collections::HashMap;

use ::Data;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;
use dataflow::channels::pact::Exchange;

/// Generic intra-timestamp aggregation
/// 
/// Extension method supporting aggregation of keyed data within timestamp. 
/// For inter-timestamp aggregation, consider `StateMachine`.
trait Aggregate<S: Scope, K: Data+Hash, V: Data> {
	/// Aggregates data of the form `(key, val)`, using user-supplied logic.
	///
	/// The `aggregate` method is implemented for streams of `(K, V)` data, 
	/// and takes functions `fold`, `emit`, and `hash`; used to combine new `V` 
	/// data with existing `D` state, to produce `R` output from `D` state, and 
	/// to route `K` keys, respectively.
	///
	/// Aggregation happens within each time, and results are produced once the 
	/// time is complete.
	fn aggregate<
		R: Data, 
		D: Default+'static, 
		F: Fn(&K, V, &mut D)+'static, 
		G: Fn(K, D)->R+'static,
		H: Fn(&K)->u64+'static,
	>(&self, fold: F, emit: G, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq;
} 

impl<S: Scope, K: Data+Hash+Eq, V: Data> Aggregate<S, K, V> for Stream<S, (K, V)> {

		fn aggregate<
			R: Data, 
			D: Default+'static, 
			F: Fn(&K, V, &mut D)+'static, 
			G: Fn(K, D)->R+'static,
			H: Fn(&K)->u64+'static,
		>(&self, fold: F, emit: G, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq {

		let mut aggregates = HashMap::new(); 

		self.unary_notify(Exchange::new(move |&(ref k, _)| hash(k)), "Aggregate", vec![], move |input, output, notificator| {

			// read each input, fold into aggregates
			while let Some((time, data)) = input.next() {
				let agg_time = aggregates.entry(time.time()).or_insert(HashMap::new());
				for (key, val) in data.drain(..) {
					let agg = agg_time.entry(key.clone()).or_insert(Default::default());
					fold(&key, val, agg);
				}
				notificator.notify_at(time);
			}

			// pop completed aggregates, send along whatever
			while let Some((time, _)) = notificator.next() {
				if let Some(aggs) = aggregates.remove(&time.time()) {
					let mut session = output.session(&time);
					for (key, agg) in aggs {
						session.give(emit(key, agg));
					}
				}
			}
		})

	}
}

#[cfg(test)]
mod test {

	use dataflow::operators::{ToStream, Inspect, Map};
	use super::Aggregate;

    #[test]
    fn it_works() {

    	::example(|scope| {

    		(0..10).to_stream(scope)
    			   .map(|x| (x % 2, x))
    			   .aggregate(|_key, val, agg| { *agg += val; }, |key, agg: i32| (key, agg), |key| *key as u64)
    			   .inspect(|x| assert!(*x == (0, 20) || *x == (1, 25)));

       		(0..10).to_stream(scope)
    			   .map(|x| (x % 2, x))
    			   .aggregate::<_,Vec<i32>,_,_,_>(|_key, val, agg| { agg.push(val); }, |key, agg| (key, agg.len()), |key| *key as u64)
    			   .inspect(|x| assert!(*x == (0, 5) || *x == (1, 5)));

    	})

    }
}
