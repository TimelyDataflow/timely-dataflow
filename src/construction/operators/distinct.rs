use std::collections::{HashMap, HashSet};
use std::collections::hash_state::DefaultState;
use std::hash::{hash, Hash, SipHasher};
use std::default::Default;

use communication::Data;
use communication::pact::Exchange;

use construction::{Stream, GraphBuilder};
use construction::operators::unary::UnaryNotifyExt;

use serialization::Serializable;

pub trait DistinctExtensionTrait {
    fn distinct(&self) -> Self;
    fn distinct_batch(&self) -> Self;
}

impl<G: GraphBuilder, D: Data+Hash+Eq+Serializable> DistinctExtensionTrait for Stream<G, D>
where G::Timestamp: Hash {

    fn distinct(&self) -> Stream<G, D> {
        let mut elements: HashMap<_, HashSet<_, DefaultState<SipHasher>>> = HashMap::new();
        let exch = Exchange::new(|x| hash::<_,SipHasher>(&x));
        self.unary_notify(exch, "Distinct", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                let set = elements.entry(time).or_insert(Default::default());
                let mut session = output.session(&time);
                for datum in data.drain(..) {
                    if set.insert(datum.clone()) {
                        session.give(datum);
                    }
                }

                notificator.notify_at(&time);
            }

            while let Some((time, _count)) = notificator.next() {
                elements.remove(&time);
            }
        })
    }

    fn distinct_batch(&self) -> Stream<G, D> {
        let mut elements: HashMap<_, HashSet<_, DefaultState<SipHasher>>> = HashMap::new();
        let exch = Exchange::new(|x| hash::<_,SipHasher>(&x));
        self.unary_notify(exch, "DistinctBlock", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {
                let set = elements.entry(time).or_insert(Default::default());
                for datum in data.drain(..) { set.insert(datum); }

                notificator.notify_at(&time);
            }

            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = elements.remove(&time) {
                    output.give_at(&time, data.drain());
                }
            }
        })
    }
}
