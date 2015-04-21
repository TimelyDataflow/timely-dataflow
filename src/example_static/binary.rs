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


pub trait BinaryExt<G: GraphBuilder, D1: Data> {
    fn binary<D2: Data, D3: Data,
             L: FnMut(&mut BinaryScopeHandle<G::Timestamp, D1, D2, D3, P1::Pullable, P2::Pullable>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
            (self, Stream<G::Timestamp, D2>, pact1: P1, pact2: P2, name: String, logic: L) -> ActiveStream<G, D3>;
}

impl<G: GraphBuilder, D1: Data> BinaryExt<G, D1> for ActiveStream<G, D1> {
    fn binary<
             D2: Data,
             D3: Data,
             L: FnMut(&mut BinaryScopeHandle<G::Timestamp, D1, D2, D3, P1::Pullable, P2::Pullable>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (mut self, other: Stream<G::Timestamp, D2>, pact1: P1, pact2: P2, name: String, logic: L) -> ActiveStream<G, D3> {
        let (sender1, receiver1) = pact1.connect(self.builder.communicator());
        let (sender2, receiver2) = pact2.connect(self.builder.communicator());;
        let (targets, registrar) = OutputPort::<G::Timestamp,D3>::new();
        let scope = BinaryScope::new(receiver1, receiver2, targets, name, logic);
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
                       L: FnMut(&mut BinaryScopeHandle<T, D1, D2, D3, P1, P2>)> {
    name:           String,
    handle:         BinaryScopeHandle<T, D1, D2, D3, P1, P2>,
    logic:          L,
}

impl<T: Timestamp,
     D1: Data, D2: Data, D3: Data,
     P1: Pullable<(T, Vec<D1>)>,
     P2: Pullable<(T, Vec<D2>)>,
     L: FnMut(&mut BinaryScopeHandle<T, D1, D2, D3, P1, P2>)> BinaryScope<T, D1, D2, D3, P1, P2, L> {
    pub fn new(receiver1: P1, receiver2: P2, targets: OutputPort<T, D3>, name: String, logic: L) -> BinaryScope<T, D1, D2, D3, P1, P2, L> {
        BinaryScope {
            name: name,
            handle: BinaryScopeHandle {
                input1:      PullableHelper::new(receiver1),
                input2:      PullableHelper::new(receiver2),
                output:      ObserverHelper::new(targets, Rc::new(RefCell::new(CountMap::new()))),
                notificator: Default::default(),
            },
            logic: logic,
        }
    }
}

impl<T, D1, D2, D3, P1, P2, L> Scope<T> for BinaryScope<T, D1, D2, D3, P1, P2, L>
where T: Timestamp,
      D1: Data, D2: Data, D3: Data,
      P1: Pullable<(T, Vec<D1>)>,
      P2: Pullable<(T, Vec<D2>)>,
      L: FnMut(&mut BinaryScopeHandle<T, D1, D2, D3, P1, P2>) {
    fn inputs(&self) -> u64 { 2 }
    fn outputs(&self) -> u64 { 1 }

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
        (self.logic)(&mut self.handle);

        // extract what we know about progress from the input and output adapters.
        self.handle.input1.pull_progress(&mut consumed[0]);
        self.handle.input2.pull_progress(&mut consumed[1]);
        self.handle.output.pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("{}", self.name) }
    fn notify_me(&self) -> bool { true }
}
