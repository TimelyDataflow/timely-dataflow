use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::hash::{Hash, SipHasher};
use std::collections::hash_state::DefaultState;
use std::hash;
use core::fmt::Debug;

use std::rc::Rc;
use std::cell::RefCell;

use std::default::Default;

use progress::count_map::CountMap;
use progress::graph::GraphExtension;

use progress::notificator::Notificator;
use progress::{Timestamp, PathSummary, Scope, Graph};
use communication::channels::{Data};
use communication::exchange::{ExchangeReceiver, exchange_with};
use example::stream::Stream;
use communication::channels::{OutputPort, ObserverHelper};
use communication::Observer;
use communication::observer::ObserverSessionExt;

use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

pub trait DistinctExtensionTrait { fn distinct(&mut self) -> Self; }

impl<G: Graph, D: Data+Hash<SipHasher>+Eq+Debug> DistinctExtensionTrait for Stream<G, D> {
    fn distinct(&mut self) -> Stream<G, D> {
        let (sender, receiver) = { exchange_with(&mut (*self.allocator.borrow_mut()), |x| hash::hash(&x)) };
        let targets: OutputPort<G::Timestamp,D> = Default::default();

        let index = self.graph.add_scope(DistinctScope {
            input:          receiver,
            output:         ObserverHelper::new(targets.clone(), Rc::new(RefCell::new(CountMap::new()))),
            elements:       HashMap::new(),
            notificator:    Default::default(),
        });

        self.graph.connect(self.name, ScopeInput(index, 0));
        self.add_observer(sender);

        Stream {
            name: ScopeOutput(index, 0),
            ports: targets,
            graph: self.graph.clone(),
            allocator: self.allocator.clone(),
        }
    }
}

pub struct DistinctScope<T: Timestamp, D: Data+Hash<SipHasher>+Eq+PartialEq> {
    input:          ExchangeReceiver<T, D>,
    output:         ObserverHelper<OutputPort<T, D>>,
    elements:       HashMap<T, HashSet<D, DefaultState<SipHasher>>>,
    notificator:    Notificator<T>,
}

impl<T: Timestamp, S: PathSummary<T>, D: Data+Hash<SipHasher>+Eq+PartialEq+Debug> Scope<T, S> for DistinctScope<T, D> {
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn push_external_progress(&mut self, external: &mut Vec<CountMap<T>>) -> () {
        while let Some((ref time, val)) = external[0].pop() { self.notificator.update_frontier(time, val); }
    }

    fn pull_internal_progress(&mut self, internal: &mut Vec<CountMap<T>>,
                                         consumed: &mut Vec<CountMap<T>>,
                                         produced: &mut Vec<CountMap<T>>) -> bool
    {
        // drain the input into sets.
        while let Some((time, data)) = self.input.next() {
        // for (time, data) in self.input {
            let set = match self.elements.entry(time) {
                Occupied(x) => x.into_mut(),
                Vacant(x)   => {
                    internal[0].update(&time, 1);
                    self.notificator.notify_at(&time);
                    x.insert(HashSet::with_hash_state(Default::default()))
                },
            };

            for datum in data.into_iter() { set.insert(datum); }
        }

        // // send anything for times we have finalized
        while let Some((time, _count)) = self.notificator.next() {
        // for (time, _count) in self.notificator {
            if let Some(data) = self.elements.remove(&time) {
                let mut session = self.output.session(&time);
                for datum in data.into_iter() { session.push(&datum); }
            }
            internal[0].update(&time, -1);
        }

        // extract what we know about progress from the input and output adapters.
        self.input.pull_progress(&mut consumed[0], &mut internal[0]);
        self.output.pull_progress(&mut produced[0]);

        // println!("Distinct pulled: i: {}, c: {}, p; {}", internal[0].len(), consumed[0].len(), produced[0].len());

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("Distinct") }
    fn notify_me(&self) -> bool { true }
}
