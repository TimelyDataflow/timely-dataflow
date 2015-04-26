use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;
use std::marker::PhantomData;

use progress::nested::subgraph::Source::ScopeOutput;
use progress::nested::subgraph::Target::ScopeInput;

use communication::*;
use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Scope, Antichain};
use communication::channels::ObserverHelper;
use communication::pact::PactPullable;

use example_static::stream::ActiveStream;
use example_static::builder::*;

pub struct PullableHelper<T:Timestamp, D: Data, P: Pullable<(T, Vec<D>)>> {
    receiver:   PactPullable<T, D, P>,
    consumed:   CountMap<T>,
    phantom:    PhantomData<D>,
}

impl<T:Timestamp, D: Data, P: Pullable<(T, Vec<D>)>> PullableHelper<T, D, P> {
    pub fn pull(&mut self) -> Option<(T, &mut Vec<D>)> {
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

impl<T:Timestamp, D:Data, P: Pullable<(T, Vec<D>)>> PullableHelper<T, D, P> {
    pub fn new(input: PactPullable<T, D, P>) -> PullableHelper<T, D, P> {
        PullableHelper {
            receiver: input,
            consumed: CountMap::new(),
            phantom:  PhantomData,
        }
    }
    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>) {
        while let Some((ref time, value)) = self.consumed.pop() {
            consumed.update(time, value);
        }
    }
}


pub trait UnaryNotifyExt<G: GraphBuilder, D1: Data> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut PullableHelper<G::Timestamp, D1, P::Pullable>,
                     &mut ObserverHelper<OutputPort<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
            (self, pact: P, name: String, init: Vec<G::Timestamp>, logic: L) -> ActiveStream<G, D2>;
}

impl<G: GraphBuilder, D1: Data> UnaryNotifyExt<G, D1> for ActiveStream<G, D1> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut PullableHelper<G::Timestamp, D1, P::Pullable>,
                     &mut ObserverHelper<OutputPort<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (mut self, pact: P, name: String, init: Vec<G::Timestamp>, logic: L) -> ActiveStream<G, D2> {
        let (sender, receiver) = pact.connect(self.builder.communicator());
        let (targets, registrar) = OutputPort::<G::Timestamp,D2>::new();
        let scope = UnaryScope::new(receiver, targets, name, logic, Some((init, self.builder.communicator().peers())));
        let index = self.builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);
        self.transfer_borrow_to(ScopeOutput(index, 0), registrar)
    }
}


pub trait UnaryStreamExt<G: GraphBuilder, D1: Data> {
    fn unary_stream<D2: Data,
             L: FnMut(&mut PullableHelper<G::Timestamp, D1, P::Pullable>,
                      &mut ObserverHelper<OutputPort<G::Timestamp, D2>>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
            (self, pact: P, name: String, logic: L) -> ActiveStream<G, D2>;
}

impl<G: GraphBuilder, D1: Data> UnaryStreamExt<G, D1> for ActiveStream<G, D1> {
    fn unary_stream<D2: Data,
             L: FnMut(&mut PullableHelper<G::Timestamp, D1, P::Pullable>,
                      &mut ObserverHelper<OutputPort<G::Timestamp, D2>>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (mut self, pact: P, name: String, mut logic: L) -> ActiveStream<G, D2> {
        let (sender, receiver) = pact.connect(self.builder.communicator());
        let (targets, registrar) = OutputPort::<G::Timestamp,D2>::new();
        let scope = UnaryScope::new(receiver, targets, name, move |i,o,_| logic(i,o), None);
        let index = self.builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);
        self.transfer_borrow_to(ScopeOutput(index, 0), registrar)
    }
}

pub struct UnaryScopeHandle<T: Timestamp, D1: Data, D2: Data, P: Pullable<(T, Vec<D1>)>> {
    pub input:          PullableHelper<T, D1, P>,
    pub output:         ObserverHelper<OutputPort<T, D2>>,
    pub notificator:    Notificator<T>,
}

pub struct UnaryScope
    <
    T: Timestamp,
    D1: Data,
    D2: Data,
    P: Pullable<(T, Vec<D1>)>,
    L: FnMut(&mut PullableHelper<T, D1, P>,
             &mut ObserverHelper<OutputPort<T, D2>>,
             &mut Notificator<T>)> {
    name:           String,
    handle:         UnaryScopeHandle<T, D1, D2, P>,
    logic:          L,
    notify:         Option<(Vec<T>, u64)>,    // initial notifications and peers
}

impl<T:  Timestamp,
     D1: Data,
     D2: Data,
     P:  Pullable<(T, Vec<D1>)>,
     L:  FnMut(&mut PullableHelper<T, D1, P>,
               &mut ObserverHelper<OutputPort<T, D2>>,
               &mut Notificator<T>)>
UnaryScope<T, D1, D2, P, L> {
    pub fn new(receiver: PactPullable<T, D1, P>,
               targets:  OutputPort<T, D2>,
               name:     String,
               logic:    L,
               notify:   Option<(Vec<T>, u64)>)
           -> UnaryScope<T, D1, D2, P, L> {
        UnaryScope {
            name:    name,
            handle:  UnaryScopeHandle {
                input:       PullableHelper::new(receiver),
                output:      ObserverHelper::new(targets, Rc::new(RefCell::new(CountMap::new()))),
                notificator: Default::default(),
            },
            logic:   logic,
            notify:  notify,
        }
    }
}

impl<T, D1, D2, P, L> Scope<T> for UnaryScope<T, D1, D2, P, L>
where T: Timestamp,
      D1: Data, D2: Data,
      P: Pullable<(T, Vec<D1>)>,
      L: FnMut(&mut PullableHelper<T, D1, P>,
               &mut ObserverHelper<OutputPort<T, D2>>,
               &mut Notificator<T>) {
    fn inputs(&self) -> u64 { 1 }
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

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>,
                                       frontier: &mut [CountMap<T>]) -> () {
        self.handle.notificator.update_frontier_from_cm(frontier);
    }

    fn push_external_progress(&mut self, external: &mut [CountMap<T>]) -> () {
        self.handle.notificator.update_frontier_from_cm(external);
    }

    fn pull_internal_progress(&mut self, internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool
    {
        (self.logic)(&mut self.handle.input, &mut self.handle.output, &mut self.handle.notificator);

        // extract what we know about progress from the input and output adapters.
        self.handle.input.pull_progress(&mut consumed[0]);
        self.handle.output.pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("{}", self.name) }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
