use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::nested::subgraph::Source::ScopeOutput;
use progress::nested::subgraph::Target::ScopeInput;

use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Scope, Antichain};

use communication::{Data, Pullable};
use communication::observer::Counter as ObserverCounter;
use communication::observer::tee::Tee;
use communication::pullable::Counter as PullableCounter;
use communication::pact::ParallelizationContract;

use example_shared::*;

use drain::DrainExt;

pub trait BinaryStreamExt<G: GraphBuilder, D1: Data> {
    fn binary_stream<D2: Data,
              D3: Data,
              L: FnMut(&mut PullableCounter<G::Timestamp, D1, P1::Pullable>,
                       &mut PullableCounter<G::Timestamp, D2, P2::Pullable>,
                       &mut ObserverCounter<Tee<G::Timestamp, D3>>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: String, logic: L) -> Stream<G, D3>;
}

impl<G: GraphBuilder, D1: Data> BinaryStreamExt<G, D1> for Stream<G, D1> {
    fn binary_stream<
             D2: Data,
             D3: Data,
             L: FnMut(&mut PullableCounter<G::Timestamp, D1, P1::Pullable>,
                      &mut PullableCounter<G::Timestamp, D2, P2::Pullable>,
                      &mut ObserverCounter<Tee<G::Timestamp, D3>>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: String, mut logic: L) -> Stream<G, D3> {

        let mut builder = self.builder();

        let (sender1, receiver1) = pact1.connect(&mut builder);
        let (sender2, receiver2) = pact2.connect(&mut builder);;
        let (targets, registrar) = Tee::<G::Timestamp,D3>::new();
        let scope = BinaryScope::new(receiver1, receiver2, targets, name, None, move |i1, i2, o, _| logic(i1, i2, o));
        let index = builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender1);
        other.connect_to(ScopeInput(index, 1), sender2);
        // self.builder.connect(other.name, ScopeInput(index, 1));
        // other.ports.add_observer(sender2);

        Stream::new(ScopeOutput(index, 0), registrar, builder)
    }
}

pub trait BinaryNotifyExt<G: GraphBuilder, D1: Data> {
    fn binary_notify<D2: Data,
              D3: Data,
              L: FnMut(&mut PullableCounter<G::Timestamp, D1, P1::Pullable>,
                       &mut PullableCounter<G::Timestamp, D2, P2::Pullable>,
                       &mut ObserverCounter<Tee<G::Timestamp, D3>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: String, notify: Vec<G::Timestamp>, logic: L) -> Stream<G, D3>;
}

impl<G: GraphBuilder, D1: Data> BinaryNotifyExt<G, D1> for Stream<G, D1> {
    fn binary_notify<
             D2: Data,
             D3: Data,
             L: FnMut(&mut PullableCounter<G::Timestamp, D1, P1::Pullable>,
                      &mut PullableCounter<G::Timestamp, D2, P2::Pullable>,
                      &mut ObserverCounter<Tee<G::Timestamp, D3>>,
                      &mut Notificator<G::Timestamp>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: String, notify: Vec<G::Timestamp>, logic: L) -> Stream<G, D3> {

        let mut builder = self.builder();

        let (sender1, receiver1) = pact1.connect(&mut builder);
        let (sender2, receiver2) = pact2.connect(&mut builder);;
        let (targets, registrar) = Tee::<G::Timestamp,D3>::new();
        let scope = BinaryScope::new(receiver1, receiver2, targets, name, Some((notify, builder.peers())), logic);
        let index = builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender1);
        other.connect_to(ScopeInput(index, 1), sender2);
        // self.builder.connect(other.name, ScopeInput(index, 1));
        // other.ports.add_observer(sender2);

        Stream::new(ScopeOutput(index, 0), registrar, builder)
    }
}

pub struct BinaryScopeHandle<T: Timestamp, D1: Data, D2: Data, D3: Data, P1: Pullable<T, D1>, P2: Pullable<T, D2>> {
    pub input1:         PullableCounter<T, D1, P1>,
    pub input2:         PullableCounter<T, D2, P2>,
    pub output:         ObserverCounter<Tee<T, D3>>,
    pub notificator:    Notificator<T>,
}

pub struct BinaryScope<T: Timestamp,
                       D1: Data, D2: Data, D3: Data,
                       P1: Pullable<T, D1>,
                       P2: Pullable<T, D2>,
                       L: FnMut(&mut PullableCounter<T, D1, P1>,
                                &mut PullableCounter<T, D2, P2>,
                                &mut ObserverCounter<Tee<T, D3>>,
                                &mut Notificator<T>)> {
    name:           String,
    handle:         BinaryScopeHandle<T, D1, D2, D3, P1, P2>,
    logic:          L,
    notify:         Option<(Vec<T>, u64)>,  // initial notifications and peers
}

impl<T: Timestamp,
     D1: Data, D2: Data, D3: Data,
     P1: Pullable<T, D1>,
     P2: Pullable<T, D2>,
     L: FnMut(&mut PullableCounter<T, D1, P1>,
              &mut PullableCounter<T, D2, P2>,
              &mut ObserverCounter<Tee<T, D3>>,
              &mut Notificator<T>)+'static>
BinaryScope<T, D1, D2, D3, P1, P2, L> {
    pub fn new(receiver1: P1,
               receiver2: P2,
               targets: Tee<T, D3>,
               name: String,
               notify: Option<(Vec<T>, u64)>,
               logic: L)
        -> BinaryScope<T, D1, D2, D3, P1, P2, L> {
        BinaryScope {
            name: name,
            handle: BinaryScopeHandle {
                input1:      PullableCounter::new(receiver1),
                input2:      PullableCounter::new(receiver2),
                output:      ObserverCounter::new(targets, Rc::new(RefCell::new(CountMap::new()))),
                notificator: Default::default(),
            },
            logic: logic,
            notify: notify,
        }
    }
}

impl<T, D1, D2, D3, P1, P2, L> Scope<T> for BinaryScope<T, D1, D2, D3, P1, P2, L>
where T: Timestamp,
      D1: Data, D2: Data, D3: Data,
      P1: Pullable<T, D1>,
      P2: Pullable<T, D2>,
      L: FnMut(&mut PullableCounter<T, D1, P1>,
               &mut PullableCounter<T, D2, P2>,
               &mut ObserverCounter<Tee<T, D3>>,
               &mut Notificator<T>)+'static {
    fn inputs(&self) -> u64 { 2 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        let mut internal = vec![CountMap::new()];
        if let Some((ref mut initial, peers)) = self.notify {
            for time in initial.drain_temp() {
                for _ in (0..peers) {
                    self.handle.notificator.notify_at(&time);
                }
            }

            self.handle.notificator.pull_progress(&mut internal[0]);
        }
        (vec![vec![Antichain::from_elem(Default::default())],
              vec![Antichain::from_elem(Default::default())]], internal)
    }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, frontier: &mut [CountMap<T>]) -> () {
        self.handle.notificator.update_frontier_from_cm(frontier);
    }

    fn push_external_progress(&mut self, external: &mut [CountMap<T>]) -> () {
        self.handle.notificator.update_frontier_from_cm(external);
    }

    fn pull_internal_progress(&mut self, internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool
    {
        (self.logic)(&mut self.handle.input1, &mut self.handle.input2, &mut self.handle.output, &mut self.handle.notificator);

        // extract what we know about progress from the input and output adapters.
        self.handle.input1.pull_progress(&mut consumed[0]);
        self.handle.input2.pull_progress(&mut consumed[1]);
        self.handle.output.pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("{}", self.name) }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
