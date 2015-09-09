use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::nested::subgraph::Source::ChildOutput;
use progress::nested::subgraph::Target::ChildInput;

use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Antichain, Operate};

// use communication::{Data, Pullable};
// use fabric::{Pull, Push};
use ::Data;
// use communication::message::Message;
use dataflow::channels::pushers::counter::Counter as PushCounter;
use dataflow::channels::pushers::tee::Tee;
use dataflow::channels::pullers::counter::Counter as PullCounter;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pushers::buffer::Buffer as ObserverBuffer;

use dataflow::{Stream, Scope};

use drain::DrainExt;

pub trait Binary<G: Scope, D1: Data> {
    fn binary_stream<D2: Data,
              D3: Data,
              L: FnMut(&mut PullCounter<G::Timestamp, D1>,
                       &mut PullCounter<G::Timestamp, D2>,
                       &mut ObserverBuffer<G::Timestamp, D3, PushCounter<G::Timestamp, D3, Tee<G::Timestamp, D3>>>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, logic: L) -> Stream<G, D3>;
    fn binary_notify<D2: Data,
              D3: Data,
              L: FnMut(&mut PullCounter<G::Timestamp, D1>,
                       &mut PullCounter<G::Timestamp, D2>,
                       &mut ObserverBuffer<G::Timestamp, D3, PushCounter<G::Timestamp, D3, Tee<G::Timestamp, D3>>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, notify: Vec<G::Timestamp>, logic: L) -> Stream<G, D3>;
}

impl<G: Scope, D1: Data> Binary<G, D1> for Stream<G, D1> {
    fn binary_stream<
             D2: Data,
             D3: Data,
             L: FnMut(&mut PullCounter<G::Timestamp, D1>,
                      &mut PullCounter<G::Timestamp, D2>,
                      &mut ObserverBuffer<G::Timestamp, D3, PushCounter<G::Timestamp, D3, Tee<G::Timestamp, D3>>>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, mut logic: L) -> Stream<G, D3> {

        let mut scope = self.scope();
        let channel_id1 = scope.new_identifier();
        let channel_id2 = scope.new_identifier();

        let (sender1, receiver1) = pact1.connect(&mut scope, channel_id1);
        let (sender2, receiver2) = pact2.connect(&mut scope, channel_id2);;
        let (targets, registrar) = Tee::<G::Timestamp,D3>::new();
        let operator = Operator::new(PullCounter::new(receiver1), PullCounter::new(receiver2), targets, name.to_owned(), None, move |i1, i2, o, _| logic(i1, i2, o));
        let index = scope.add_operator(operator);
        self.connect_to(ChildInput(index, 0), sender1, channel_id1);
        other.connect_to(ChildInput(index, 1), sender2, channel_id2);
        // self.scope.connect(other.name, ChildInput(index, 1));
        // other.ports.add_observer(sender2);

        Stream::new(ChildOutput(index, 0), registrar, scope)
    }

    fn binary_notify<
             D2: Data,
             D3: Data,
             L: FnMut(&mut PullCounter<G::Timestamp, D1>,
                      &mut PullCounter<G::Timestamp, D2>,
                      &mut ObserverBuffer<G::Timestamp, D3, PushCounter<G::Timestamp, D3, Tee<G::Timestamp, D3>>>,
                      &mut Notificator<G::Timestamp>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, notify: Vec<G::Timestamp>, logic: L) -> Stream<G, D3> {

        let mut scope = self.scope();
        let channel_id1 = scope.new_identifier();
        let channel_id2 = scope.new_identifier();

        let (sender1, receiver1) = pact1.connect(&mut scope, channel_id1);
        let (sender2, receiver2) = pact2.connect(&mut scope, channel_id2);;
        let (targets, registrar) = Tee::<G::Timestamp,D3>::new();
        let operator = Operator::new(PullCounter::new(receiver1), PullCounter::new(receiver2), targets, name.to_owned(), Some((notify, scope.peers())), logic);
        let index = scope.add_operator(operator);
        self.connect_to(ChildInput(index, 0), sender1, channel_id1);
        other.connect_to(ChildInput(index, 1), sender2, channel_id2);
        // self.scope.connect(other.name, ChildInput(index, 1));
        // other.ports.add_observer(sender2);

        Stream::new(ChildOutput(index, 0), registrar, scope)
    }
}

struct Handle<T: Timestamp, D1: Data, D2: Data, D3: Data> {
    pub input1:         PullCounter<T, D1>,
    pub input2:         PullCounter<T, D2>,
    pub output:         ObserverBuffer<T, D3, PushCounter<T, D3, Tee<T, D3>>>,
    pub notificator:    Notificator<T>,
}

struct Operator<T: Timestamp,
                       D1: Data, D2: Data, D3: Data,
                       L: FnMut(&mut PullCounter<T, D1>,
                                &mut PullCounter<T, D2>,
                                &mut ObserverBuffer<T, D3, PushCounter<T, D3, Tee<T, D3>>>,
                                &mut Notificator<T>)> {
    name:           String,
    handle:         Handle<T, D1, D2, D3>,
    logic:          L,
    notify:         Option<(Vec<T>, usize)>,  // initial notifications and peers
}

impl<T: Timestamp,
     D1: Data, D2: Data, D3: Data,
     L: FnMut(&mut PullCounter<T, D1>,
              &mut PullCounter<T, D2>,
              &mut ObserverBuffer<T, D3, PushCounter<T, D3, Tee<T, D3>>>,
              &mut Notificator<T>)+'static>
Operator<T, D1, D2, D3, L> {
    pub fn new(receiver1: PullCounter<T, D1>,
               receiver2: PullCounter<T, D2>,
               targets: Tee<T, D3>,
               name: String,
               notify: Option<(Vec<T>, usize)>,
               logic: L)
        -> Operator<T, D1, D2, D3, L> {
        Operator {
            name: name,
            handle: Handle {
                input1:      receiver1,
                input2:      receiver2,
                output:      ObserverBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
                notificator: Default::default(),
            },
            logic: logic,
            notify: notify,
        }
    }
}

impl<T, D1, D2, D3, L> Operate<T> for Operator<T, D1, D2, D3, L>
where T: Timestamp,
      D1: Data, D2: Data, D3: Data,
      L: FnMut(&mut PullCounter<T, D1>,
               &mut PullCounter<T, D2>,
               &mut ObserverBuffer<T, D3, PushCounter<T, D3, Tee<T, D3>>>,
               &mut Notificator<T>)+'static {
    fn inputs(&self) -> usize { 2 }
    fn outputs(&self) -> usize { 1 }

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

        self.handle.output.cease();

        // extract what we know about progress from the input and output adapters.
        self.handle.input1.pull_progress(&mut consumed[0]);
        self.handle.input2.pull_progress(&mut consumed[1]);
        self.handle.output.inner().pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> &str { &self.name }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
