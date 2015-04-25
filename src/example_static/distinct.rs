use std::collections::{HashMap, HashSet};
use std::collections::hash_state::DefaultState;
use std::hash::{hash, Hash, SipHasher};
use std::default::Default;

use communication::*;
use communication::pact::Exchange;

use example_static::stream::ActiveStream;
use example_static::unary::*;
use example_static::builder::*;

use columnar::Columnar;

pub trait DistinctExtensionTrait { fn distinct(self) -> Self; }

impl<G: GraphBuilder, D: Data+Hash+Eq+Columnar> DistinctExtensionTrait for ActiveStream<G, D>
where G::Timestamp: Hash {
    fn distinct(self) -> ActiveStream<G, D> {
        let mut elements: HashMap<_, HashSet<_, DefaultState<SipHasher>>> = HashMap::new();
        let exch = Exchange::new(|x| hash::<_,SipHasher>(&x));
        self.unary_notify(exch, format!("Distinct"), vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.pull() {            // read inputs
                let set = elements.entry(time).or_insert_with(|| {          // look up time
                    notificator.notify_at(&time);                           // notify if new
                    Default::default()                                      // default HashSet
                });

                for datum in data.drain() { set.insert(datum); }            // add data to set
            }
            // for each available notification, send corresponding set
            while let Some((time, _count)) = notificator.next() {           // pull notifications
                if let Some(data) = elements.remove(&time) {                // find the set
                    output.give_at(&time, data.into_iter());         // send the records
                }
            }
        })
    }
}
