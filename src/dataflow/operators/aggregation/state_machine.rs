use std::hash::Hash;
use std::collections::HashMap;

use ::Data;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;
use dataflow::channels::pact::Exchange;

/// Generic state-transition machinery: each key has a state, and receives a sequence of events. 
/// Events are applied in time-order, but no other promises are made. Each state transition can
/// produce output, which is sent. 
///
/// state_machine will buffer inputs if earlier inputs may still arrive. it will directly apply 
/// updates for the current time reflected in the notificator, though. In the case of partially 
/// ordered times, the only guarantee is that updates are not applied out of order, not that there
/// is some total order on times respecting the total order (updates may be interleaved).


trait StateMachine<S: Scope, K: Data+Hash+Eq, V: Data> {
	fn state_machine<
		R: Data, 							// output type
		D: Default+'static, 						// per-key state (data)
		I: Iterator<Item=R>,				// type of output iterator
		F: Fn(&K, V, &mut D)->(bool, I)+'static, 	// state update logic
		H: Fn(&K)->u64+'static,						// "hash" function for keys
	>(&self, fold: F, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq ;
} 

impl<S: Scope, K: Data+Hash+Eq, V: Data> StateMachine<S, K, V> for Stream<S, (K, V)> {
	fn state_machine<
			R: Data, 							// output type
			D: Default+'static, 						// per-key state (data)
			I: Iterator<Item=R>,				// type of output iterator
			F: Fn(&K, V, &mut D)->(bool, I)+'static, 	// state update logic
			H: Fn(&K)->u64+'static,						// "hash" function for keys
		>(&self, fold: F, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq {

		let mut pending = HashMap::new(); 	// times -> (keys -> state)
		let mut states = HashMap::new();	// keys -> state

		self.unary_notify(Exchange::new(move |&(ref k, _)| hash(k)), "StateMachine", vec![], move |input, output, notificator| {

			// stash each input and request a notification when ready
			while let Some((time, data)) = input.next() {
				// stash if not time yet
				if notificator.frontier(0).iter().any(|x| x.lt(&time.time())) {
					pending.entry(time.time()).or_insert(Vec::new()).extend(data.drain(..));
					notificator.notify_at(time);
				}
				else {
					// else we can process immediately
					let mut session = output.session(&time);
					for (key, val) in data.drain(..) {
						let (remove, output) = { 
							let state = states.entry(key.clone()).or_insert(Default::default());
							fold(&key, val, state)
						};
						if remove { states.remove(&key); }
						session.give_iterator(output);
					}
				}
			}

			// go through each time with data, process each (key, val) pair.
			while let Some((time, _)) = notificator.next() {
				if let Some(pend) = pending.remove(&time.time()) {
					let mut session = output.session(&time);
					for (key, val) in pend {
						let (remove, output) = {
							let state = states.entry(key.clone()).or_insert(Default::default());
							fold(&key, val, state)
						};
						if remove { states.remove(&key); }
						session.give_iterator(output);
					}
				}
			}

		})
	}
}
