use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::nested::subgraph::Source::ScopeOutput;
use progress::nested::subgraph::Target::ScopeInput;

use communication::*;
use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Scope, Antichain};
use communication::channels::ObserverHelper;

use example_static::builder::*;
use example_static::unary::PullableHelper;
use example_static::stream::{ActiveStream, Stream};



pub trait BinaryStreamExt<G: GraphBuilder, D1: Data> {
    fn binary_stream<D2: Data,
              D3: Data,
              L: FnMut(&mut PullableHelper<G::Timestamp, D1, P1::Pullable>,
                       &mut PullableHelper<G::Timestamp, D2, P2::Pullable>,
                       &mut ObserverHelper<OutputPort<G::Timestamp, D3>>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (self, Stream<G::Timestamp, D2>, pact1: P1, pact2: P2, name: String, logic: L) -> ActiveStream<G, D3>;
}

impl<G: GraphBuilder, D1: Data> BinaryStreamExt<G, D1> for ActiveStream<G, D1> {
    fn binary_stream<
             D2: Data,
             D3: Data,
             L: FnMut(&mut PullableHelper<G::Timestamp, D1, P1::Pullable>,
                      &mut PullableHelper<G::Timestamp, D2, P2::Pullable>,
                      &mut ObserverHelper<OutputPort<G::Timestamp, D3>>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (mut self, other: Stream<G::Timestamp, D2>, pact1: P1, pact2: P2, name: String, mut logic: L) -> ActiveStream<G, D3> {
        let (sender1, receiver1) = pact1.connect(self.builder.communicator());
        let (sender2, receiver2) = pact2.connect(self.builder.communicator());;
        let (targets, registrar) = OutputPort::<G::Timestamp,D3>::new();
        let scope = BinaryScope::new(receiver1, receiver2, targets, name, None, move |i1, i2, o, _| logic(i1, i2, o));
        let index = self.builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender1);

        // other.connect_to(ScopeInput(index, 1), sender2);
        self.builder.connect(other.name, ScopeInput(index, 1));
        other.ports.add_observer(sender2);

        self.transfer_borrow_to(ScopeOutput(index, 0), registrar)
    }
}

pub trait BinaryNotifyExt<G: GraphBuilder, D1: Data> {
    fn binary_notify<D2: Data,
              D3: Data,
              L: FnMut(&mut PullableHelper<G::Timestamp, D1, P1::Pullable>,
                       &mut PullableHelper<G::Timestamp, D2, P2::Pullable>,
                       &mut ObserverHelper<OutputPort<G::Timestamp, D3>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (self, Stream<G::Timestamp, D2>, pact1: P1, pact2: P2, name: String, notify: Vec<G::Timestamp>, logic: L) -> ActiveStream<G, D3>;
}

impl<G: GraphBuilder, D1: Data> BinaryNotifyExt<G, D1> for ActiveStream<G, D1> {
    fn binary_notify<
             D2: Data,
             D3: Data,
             L: FnMut(&mut PullableHelper<G::Timestamp, D1, P1::Pullable>,
                      &mut PullableHelper<G::Timestamp, D2, P2::Pullable>,
                      &mut ObserverHelper<OutputPort<G::Timestamp, D3>>,
                      &mut Notificator<G::Timestamp>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (mut self, other: Stream<G::Timestamp, D2>, pact1: P1, pact2: P2, name: String, notify: Vec<G::Timestamp>, logic: L) -> ActiveStream<G, D3> {
        let (sender1, receiver1) = pact1.connect(self.builder.communicator());
        let (sender2, receiver2) = pact2.connect(self.builder.communicator());;
        let (targets, registrar) = OutputPort::<G::Timestamp,D3>::new();
        let scope = BinaryScope::new(receiver1, receiver2, targets, name, Some((notify, self.builder.communicator().peers())), logic);
        let index = self.builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender1);

        // other.connect_to(ScopeInput(index, 1), sender2);
        self.builder.connect(other.name, ScopeInput(index, 1));
        other.ports.add_observer(sender2);

        self.transfer_borrow_to(ScopeOutput(index, 0), registrar)
    }
}

pub struct BinaryScopeHandle<T: Timestamp, D1: Data, D2: Data, D3: Data, P1: Pullable<(T, Vec<D1>)>, P2: Pullable<(T, Vec<D2>)>> {
    pub input1:         PullableHelper<T, D1, P1>,
    pub input2:         PullableHelper<T, D2, P2>,
    pub output:         ObserverHelper<OutputPort<T, D3>>,
    pub notificator:    Notificator<T>,
}

pub struct BinaryScope<T: Timestamp,
                       D1: Data, D2: Data, D3: Data,
                       P1: Pullable<(T, Vec<D1>)>,
                       P2: Pullable<(T, Vec<D2>)>,
                       L: FnMut(&mut PullableHelper<T, D1, P1>,
                                &mut PullableHelper<T, D2, P2>,
                                &mut ObserverHelper<OutputPort<T, D3>>,
                                &mut Notificator<T>)> {
    name:           String,
    handle:         BinaryScopeHandle<T, D1, D2, D3, P1, P2>,
    logic:          L,
    notify:         Option<(Vec<T>, u64)>,  // initial notifications and peers
}

impl<T: Timestamp,
     D1: Data, D2: Data, D3: Data,
     P1: Pullable<(T, Vec<D1>)>,
     P2: Pullable<(T, Vec<D2>)>,
     L: FnMut(&mut PullableHelper<T, D1, P1>,
              &mut PullableHelper<T, D2, P2>,
              &mut ObserverHelper<OutputPort<T, D3>>,
              &mut Notificator<T>)+'static>
BinaryScope<T, D1, D2, D3, P1, P2, L> {
    pub fn new(receiver1: P1, receiver2: P2, targets: OutputPort<T, D3>, name: String, notify: Option<(Vec<T>, u64)>, logic: L)
            -> BinaryScope<T, D1, D2, D3, P1, P2, L> {
        BinaryScope {
            name: name,
            handle: BinaryScopeHandle {
                input1:      PullableHelper::new(receiver1),
                input2:      PullableHelper::new(receiver2),
                output:      ObserverHelper::new(targets, Rc::new(RefCell::new(CountMap::new()))),
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
      P1: Pullable<(T, Vec<D1>)>,
      P2: Pullable<(T, Vec<D2>)>,
      L: FnMut(&mut PullableHelper<T, D1, P1>,
               &mut PullableHelper<T, D2, P2>,
               &mut ObserverHelper<OutputPort<T, D3>>,
               &mut Notificator<T>)+'static {
    fn inputs(&self) -> u64 { 2 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        let mut internal = vec![CountMap::new()];
        if let Some((ref mut initial, peers)) = self.notify {
            for time in initial.drain() {
                for _ in (0..peers) {
                    self.handle.notificator.notify_at(&time);
                }
            }

            self.handle.notificator.pull_progress(&mut internal[0]);
        }
        (vec![vec![Antichain::from_elem(Default::default())]], internal)
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
