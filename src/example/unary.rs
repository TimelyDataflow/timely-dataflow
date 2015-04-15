use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;
use core::marker::PhantomData;

use progress::Graph;
use progress::nested::subgraph::Source::ScopeOutput;
use progress::nested::subgraph::Target::ScopeInput;

use communication::exchange::ParallelizationContract;
use example::stream::Stream;
use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Scope, Antichain};
use communication::channels::{Data, OutputPort, ObserverHelper};
use communication::Pullable;

pub struct PullableHelper<T:Eq+Clone, D, P: Pullable<(T, Vec<D>)>> {
    receiver:   P,
    consumed:   CountMap<T>,
    phantom:    PhantomData<D>,
}

impl<T:Eq+Clone, D, P: Pullable<(T, Vec<D>)>> Pullable<(T, Vec<D>)> for PullableHelper<T, D, P> {
    fn pull(&mut self) -> Option<(T, Vec<D>)> {
        if let Some((time, data)) = self.receiver.pull() {
            if data.len() > 0 {
                self.consumed.update(&time, data.len() as i64);
                Some((time, data))
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Eq+Clone, D, P: Pullable<(T, Vec<D>)>> Iterator for PullableHelper<T, D, P> {
    type Item = (T, Vec<D>);
    fn next(&mut self) -> Option<(T, Vec<D>)> { self.pull() }
}

impl<T:Timestamp, D:Data, P: Pullable<(T, Vec<D>)>> PullableHelper<T, D, P> {
    pub fn new(input: P) -> PullableHelper<T, D, P> { PullableHelper { receiver: input, consumed: CountMap::new(), phantom: PhantomData }}
    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>) {
        while let Some((ref time, value)) = self.consumed.pop() { consumed.update(time, value); }
    }
}


pub trait UnaryExt<G: Graph, D1: Data> {
    fn unary<D2: Data,
             L: FnMut(&mut UnaryScopeHandle<G::Timestamp, D1, D2, P::Pullable>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
            (&mut self, pact: P, name: String, logic: L) -> Stream<G, D2>;
}

impl<G: Graph, D1: Data> UnaryExt<G, D1> for Stream<G, D1> {
    fn unary<D2: Data,
             L: FnMut(&mut UnaryScopeHandle<G::Timestamp, D1, D2, P::Pullable>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&mut self, pact: P, name: String, logic: L) -> Stream<G, D2> {
        let (sender, receiver) = pact.connect(&mut self.graph.communicator());
        let targets = OutputPort::<G::Timestamp,D2>::new();
        let scope = UnaryScope::new(receiver, targets.clone(), name, logic);
        let index = self.graph.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);
        self.clone_with(ScopeOutput(index, 0), targets)
    }
}

pub struct UnaryScopeHandle<T: Timestamp, D1: Data, D2: Data, P: Pullable<(T, Vec<D1>)>> {
    pub input:          PullableHelper<T, D1, P>,
    pub output:         ObserverHelper<OutputPort<T, D2>>,
    pub notificator:    Notificator<T>,
}

pub struct UnaryScope<T: Timestamp, D1: Data, D2: Data, P: Pullable<(T, Vec<D1>)>, L: FnMut(&mut UnaryScopeHandle<T, D1, D2, P>)> {
    name:           String,
    handle:         UnaryScopeHandle<T, D1, D2, P>,
    logic:          L,
}

impl<T: Timestamp, D1: Data, D2: Data, P: Pullable<(T, Vec<D1>)>, L: FnMut(&mut UnaryScopeHandle<T, D1, D2, P>)> UnaryScope<T, D1, D2, P, L> {
    pub fn new(receiver: P, targets: OutputPort<T, D2>, name: String, logic: L) -> UnaryScope<T, D1, D2, P, L> {
        UnaryScope {
            name: name,
            handle: UnaryScopeHandle {
                input:       PullableHelper::new(receiver),
                output:      ObserverHelper::new(targets, Rc::new(RefCell::new(CountMap::new()))),
                notificator: Default::default(),
            },
            logic: logic,
        }
    }
}

impl<T, D1, D2, P, L> Scope<T> for UnaryScope<T, D1, D2, P, L>
where T: Timestamp,
      D1: Data, D2: Data,
      P: Pullable<(T, Vec<D1>)>,
      L: FnMut(&mut UnaryScopeHandle<T, D1, D2, P>) {
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, frontier: &mut Vec<CountMap<T>>) -> () {
        self.handle.notificator.update_frontier_from_cm(&mut frontier[0]);
        frontier[0].clear();
    }

    fn push_external_progress(&mut self, external: &mut Vec<CountMap<T>>) -> () {
        self.handle.notificator.update_frontier_from_cm(&mut external[0]);
        external[0].clear();
    }

    fn pull_internal_progress(&mut self, internal: &mut Vec<CountMap<T>>,
                                         consumed: &mut Vec<CountMap<T>>,
                                         produced: &mut Vec<CountMap<T>>) -> bool
    {
        (self.logic)(&mut self.handle);

        // extract what we know about progress from the input and output adapters.
        self.handle.input.pull_progress(&mut consumed[0]);
        self.handle.output.pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("{}", self.name) }
    fn notify_me(&self) -> bool { true }
}
