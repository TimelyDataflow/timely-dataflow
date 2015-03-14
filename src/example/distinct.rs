use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_state::DefaultState;
use std::hash::{hash, Hash, SipHasher};
use std::default::Default;

use progress::Graph;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use communication::channels::Data;
use communication::exchange::exchange_with;
use communication::channels::OutputPort;
use communication::observer::ObserverSessionExt;
use example::stream::Stream;
use example::unary::UnaryScope;

use columnar::Columnar;

pub trait DistinctExtensionTrait { fn distinct(&mut self) -> Self; }

impl<G: Graph, D: Data+Hash+Eq+Columnar> DistinctExtensionTrait for Stream<G, D> {
    fn distinct(&mut self) -> Stream<G, D> {
        let (sender, receiver) = { exchange_with(&mut (*self.allocator.borrow_mut()), |x| hash::<_,SipHasher>(&x)) };
        let targets: OutputPort<G::Timestamp,D> = Default::default();

        // Distinct = UnaryScope + HashMap + logic
        let mut elements = HashMap::new();
        let scope = UnaryScope::new(receiver, targets.clone(), move |handle| {

            // drain the input into sets.
            // TODO : VERY IMPORTANT: if data.len() == 0, one should not be able to notify.
            // TODO : Not enforced anywhere yet, but important to correct / know about ...
            while let Some((time, data)) = handle.input.next() {
                // println!("distinct: now getting data for {:?}", time);
                let set = match elements.entry(time) {
                    Occupied(x) => x.into_mut(),
                    Vacant(x)   => {
                        handle.notificator.notify_at(&time);
                        x.insert(HashSet::<D, DefaultState<SipHasher>>::with_hash_state(Default::default()))
                    },
                };

                for datum in data.into_iter() { set.insert(datum); }
            }

            // send anything for times we have finalized
            while let Some((time, _count)) = handle.notificator.next() {
                if let Some(data) = elements.remove(&time) {
                    let mut session = handle.output.session(&time);
                    for datum in &data {
                        // println!("distinct; sent at {:?}", time);
                        session.push(datum);
                    }
                }
            }
        });

        let index = self.graph.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);
        self.clone_with(ScopeOutput(index, 0), targets)
    }
}
