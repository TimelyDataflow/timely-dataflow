use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_state::DefaultState;
use std::hash::{hash, Hash, SipHasher};
use std::default::Default;

use progress::Graph;
use communication::Communicator;
use communication::channels::Data;
use communication::exchange::exchange_with;
use communication::observer::ObserverSessionExt;
use example::stream::Stream;
use example::unary::UnaryExt;

use columnar::Columnar;

pub trait DistinctExtensionTrait { fn distinct(&mut self) -> Self; }

impl<G: Graph, D: Data+Hash+Eq+Columnar, C: Communicator> DistinctExtensionTrait for Stream<G, D, C> {
    fn distinct(&mut self) -> Stream<G, D, C> {
        let (sender, receiver) = { exchange_with(&mut (*self.allocator.borrow_mut()), |x| hash::<_,SipHasher>(&x)) };
        let mut elements: HashMap<_, HashSet<_, DefaultState<SipHasher>>> = HashMap::new();
        self.unary(sender, receiver, move |handle| {
            while let Some((time, data)) = handle.input.next() {
                let set = match elements.entry(time) {
                    Occupied(x) => { x.into_mut() },
                    Vacant(x)   => { handle.notificator.notify_at(&time);
                                     x.insert(Default::default()) },
                };

                for datum in data.into_iter() { set.insert(datum); }
            }

            while let Some((time, _count)) = handle.notificator.next() {
                if let Some(data) = elements.remove(&time) {
                    let mut session = handle.output.session(&time);
                    for datum in &data {
                        // println!("Sending {:?} at {:?}", datum, time);
                        session.push(datum);
                    }
                }
            }
        })
    }
}
