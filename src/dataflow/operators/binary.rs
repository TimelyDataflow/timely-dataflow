//! Methods to construct generic streaming and blocking binary operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::nested::subgraph::{Source, Target};

use progress::count_map::CountMap;
use dataflow::operators::Notificator;
use progress::{Timestamp, Antichain, Operate};

use ::Data;
use dataflow::channels::pushers::counter::Counter as PushCounter;
use dataflow::channels::pushers::tee::Tee;
use dataflow::channels::pullers::counter::Counter as PullCounter;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;

use dataflow::operators::{InputHandle, OutputHandle};
use dataflow::operators::capability::mint as mint_capability;

use dataflow::{Stream, Scope};

/// Methods to construct generic streaming and blocking binary operators.
pub trait Binary<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions each of its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input streams and
    /// write to the output stream.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Binary};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     let stream1 = (0..10).to_stream(scope);
    ///     let stream2 = (0..10).to_stream(scope);
    ///
    ///     stream1.binary_stream(&stream2, Pipeline, Pipeline, "example", |input1, input2, output| {
    ///         while let Some((time, data)) = input1.next() {
    ///             output.session(&time).give_content(data);
    ///         }
    ///         while let Some((time, data)) = input2.next() {
    ///             output.session(&time).give_content(data);
    ///         }
    ///     });
    /// });
    /// ```
    fn binary_stream<D2: Data,
              D3: Data,
              L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                       &mut InputHandle<G::Timestamp, D2>,
                       &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, logic: L) -> Stream<G, D3>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input streams,
    /// write to the output stream, and request and receive notifications. The method also requires
    /// a vector of the initial notifications the operator requires (commonly none).
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Binary};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     let stream1 = (0..10).to_stream(scope);
    ///     let stream2 = (0..10).to_stream(scope);
    ///
    ///     stream1.binary_notify(&stream2, Pipeline, Pipeline, "example", Vec::new(), |input1, input2, output, notificator| {
    ///         while let Some((time, data)) = input1.next() {
    ///             output.session(&time).give_content(data);
    ///             notificator.notify_at(time);
    ///         }
    ///         while let Some((time, data)) = input2.next() {
    ///             output.session(&time).give_content(data);
    ///             notificator.notify_at(time);
    ///         }
    ///         while let Some((time, count)) = notificator.next() {
    ///             println!("done with time: {:?}", time.time());
    ///         }
    ///     });
    /// });
    /// ```
    fn binary_notify<D2: Data,
              D3: Data,
              L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                       &mut InputHandle<G::Timestamp, D2>,
                       &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, notify: Vec<G::Timestamp>, logic: L) -> Stream<G, D3>;
}

impl<G: Scope, D1: Data> Binary<G, D1> for Stream<G, D1> {
    #[inline]
    fn binary_stream<
             D2: Data,
             D3: Data,
             L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                      &mut InputHandle<G::Timestamp, D2>,
                      &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
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
        self.connect_to(Target { index: index, port: 0 }, sender1, channel_id1);
        other.connect_to(Target { index: index, port: 1 }, sender2, channel_id2);
        // self.scope.connect(other.name, ChildInput(index, 1));
        // other.ports.add_observer(sender2);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }

    #[inline]
    fn binary_notify<
             D2: Data,
             D3: Data,
             L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                      &mut InputHandle<G::Timestamp, D2>,
                      &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
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
        self.connect_to(Target { index: index, port: 0 }, sender1, channel_id1);
        other.connect_to(Target { index: index, port: 1 }, sender2, channel_id2);
        // self.scope.connect(other.name, ChildInput(index, 1));
        // other.ports.add_observer(sender2);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }
}

struct Operator<T: Timestamp,
                       D1: Data, D2: Data, D3: Data,
                       L: FnMut(&mut InputHandle<T, D1>,
                                &mut InputHandle<T, D2>,
                                &mut OutputHandle<T, D3, Tee<T, D3>>,
                                &mut Notificator<T>)> {
    name:             String,
    input1:           PullCounter<T, D1>,
    input2:           PullCounter<T, D2>,
    output:           PushBuffer<T, D3, PushCounter<T, D3, Tee<T, D3>>>,
    notificator:      Notificator<T>,
    logic:            L,
    notify:           Option<(Vec<T>, usize)>,  // initial notifications and peers
    internal_changes: Rc<RefCell<CountMap<T>>>,
}

impl<T: Timestamp,
     D1: Data, D2: Data, D3: Data,
     L: FnMut(&mut InputHandle<T, D1>,
              &mut InputHandle<T, D2>,
              &mut OutputHandle<T, D3, Tee<T, D3>>,
              &mut Notificator<T>)+'static>
Operator<T, D1, D2, D3, L> {
    #[inline(always)]
    pub fn new(receiver1: PullCounter<T, D1>,
               receiver2: PullCounter<T, D2>,
               targets: Tee<T, D3>,
               name: String,
               notify: Option<(Vec<T>, usize)>,
               logic: L)
        -> Operator<T, D1, D2, D3, L> {

        Operator {
            name: name,
            input1:      receiver1,
            input2:      receiver2,
            output:      PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            notificator: Notificator::new(),
            logic: logic,
            notify: notify,
            internal_changes: Rc::new(RefCell::new(CountMap::new())),
        }
    }
}

impl<T, D1, D2, D3, L> Operate<T> for Operator<T, D1, D2, D3, L>
where T: Timestamp,
      D1: Data, D2: Data, D3: Data,
      L: FnMut(&mut InputHandle<T, D1>,
               &mut InputHandle<T, D2>,
               &mut OutputHandle<T, D3, Tee<T, D3>>,
               &mut Notificator<T>)+'static {
    fn inputs(&self) -> usize { 2 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        let mut internal = vec![CountMap::new()];
        if let Some((ref mut initial, peers)) = self.notify {
            for time in initial.drain(..) {
                for _ in 0..peers {
                    self.notificator.notify_at(mint_capability(time, self.internal_changes.clone()));
                }
            }

            self.internal_changes.borrow_mut().drain_into(&mut internal[0]);
        }
        (vec![vec![Antichain::from_elem(Default::default())],
              vec![Antichain::from_elem(Default::default())]], internal)
    }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, frontier: &mut [CountMap<T>]) -> () {
        self.notificator.update_frontier_from_cm(frontier);
    }

    fn push_external_progress(&mut self, external: &mut [CountMap<T>]) -> () {
        self.notificator.update_frontier_from_cm(external);
    }

    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<T>],
                                         internal: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool
    {
        {
            let mut input1_handle = InputHandle::new(&mut self.input1, self.internal_changes.clone());
            let mut input2_handle = InputHandle::new(&mut self.input2, self.internal_changes.clone());
            let mut output_handle = OutputHandle::new(&mut self.output);
            (self.logic)(&mut input1_handle, &mut input2_handle, &mut output_handle, &mut self.notificator);
        }

        self.output.cease();

        // extract what we know about progress from the input and output adapters.
        self.input1.pull_progress(&mut consumed[0]);
        self.input2.pull_progress(&mut consumed[1]);
        self.output.inner().pull_progress(&mut produced[0]);
        self.internal_changes.borrow_mut().drain_into(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { self.name.clone() }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
