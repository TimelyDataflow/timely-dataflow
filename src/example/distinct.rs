use std::collections::{HashMap, HashSet};
use std::collections::hash_state::DefaultState;
use std::hash::{hash, Hash, SipHasher};
use std::default::Default;

use progress::Graph;
use communication::*;
use communication::pact::Exchange;
use example::stream::Stream;
use example::unary::UnaryExt;

use columnar::Columnar;

pub trait DistinctExtensionTrait { fn distinct(&mut self) -> Self; }

impl<'a, G: Graph+'a, D: Data+Hash+Eq+Columnar> DistinctExtensionTrait for Stream<'a, G, D> {
    fn distinct(&mut self) -> Stream<'a, G, D> {
        let mut elements: HashMap<_, HashSet<_, DefaultState<SipHasher>>> = HashMap::new();
        self.unary(Exchange::new(|x| hash::<_,SipHasher>(&x)), format!("Distinct"), move |handle| {
            // read input data into sets, request notifications
            while let Some((time, data)) = handle.input.pull() {
                let set = elements.entry(time).or_insert_with(|| {
                    handle.notificator.notify_at(&time);
                    Default::default()
                });

                for datum in data.into_iter() { set.insert(datum); }
            }
            // for each available notification, send corresponding set
            while let Some((time, _count)) = handle.notificator.next() {
                if let Some(data) = elements.remove(&time) {
                    handle.output.give_at(&time, data.into_iter());
                }
            }
        })
    }
}
