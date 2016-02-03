//! Methods to construct generic streaming and blocking unary operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::nested::subgraph::{Source, Target};

use progress::count_map::CountMap;
use dataflow::operators::Notificator;
use progress::{Timestamp, Operate, Antichain};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pullers::Counter as PullCounter;

use dataflow::operators::{InputHandle, OutputHandle};
use dataflow::operators::handles::{new_input_handle, new_output_handle};
use dataflow::operators::capability::mint as mint_capability;

use ::Data;

use dataflow::{Stream, Scope};

/// Methods to construct generic streaming and blocking unary operators.
pub trait Unary<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream and
    /// write to the output stream.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Unary};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                while let Some((time, data)) = input.next() {
    ///                    output.session(&time).give_content(data);
    ///                }
    ///            });
    /// });
    /// ```
    fn unary_stream<D2, L, P> (&self, pact: P, name: &str, logic: L) -> Stream<G, D2>
    where
        D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream,
    /// write to the output stream, and request and receive notifications. The method also requires
    /// a vector of the initial notifications the operator requires (commonly none).
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Unary};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                while let Some((time, data)) = input.next() {
    ///                    output.session(&time).give_content(data);
    ///                    notificator.notify_at(time);
    ///                }
    ///                while let Some((time, count)) = notificator.next() {
    ///                    println!("done with time: {:?}", time.time());
    ///                }
    ///            });
    /// });
    /// ```
    fn unary_notify<D2, L, P> (&self, pact: P, name: &str, init: Vec<G::Timestamp>, logic: L) -> Stream<G, D2>
    where
        D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                 &mut Notificator<G::Timestamp>)+'static,
         P: ParallelizationContract<G::Timestamp, D1>;
}

impl<G: Scope, D1: Data> Unary<G, D1> for Stream<G, D1> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                     &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, init: Vec<G::Timestamp>, logic: L) -> Stream<G, D2> {

        let mut scope = self.scope();   // clones the scope

        let channel_id = scope.new_identifier();

        let (sender, receiver) = pact.connect(&mut scope, channel_id);
        let (targets, registrar) = Tee::<G::Timestamp,D2>::new();
        let operator = Operator::new(PullCounter::new(receiver), targets, name.to_owned(), logic, Some((init, scope.peers())));
        let index = scope.add_operator(operator);

        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);

        Stream::new(Source {index: index, port: 0 }, registrar, scope)
    }

    fn unary_stream<D2: Data,
             L: FnMut(&mut InputHandle<G::Timestamp, D1>,
                      &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, mut logic: L) -> Stream<G, D2> {

        let mut scope = self.scope();

        let channel_id = scope.new_identifier();

        let (sender, receiver) = pact.connect(&mut scope, channel_id);
        let (targets, registrar) = Tee::<G::Timestamp,D2>::new();
        let operator = Operator::new(PullCounter::new(receiver), targets, name.to_owned(), move |i,o,_| logic(i,o), None);
        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }
}

struct Operator
    <
    T: Timestamp,
    D1: Data,
    D2: Data,
    L: FnMut(&mut InputHandle<T, D1>,
             &mut OutputHandle<T, D2, Tee<T, D2>>,
             &mut Notificator<T>)> {
    name:             String,
    input:            PullCounter<T, D1>,
    output:           PushBuffer<T, D2, PushCounter<T, D2, Tee<T, D2>>>,
    notificator:      Notificator<T>,
    logic:            L,
    notify:           Option<(Vec<T>, usize)>,    // initial notifications and peers
    internal_changes: Rc<RefCell<CountMap<T>>>,
}

impl<T:  Timestamp,
     D1: Data,
     D2: Data,
     L:  FnMut(&mut InputHandle<T, D1>,
               &mut OutputHandle<T, D2, Tee<T, D2>>,
               &mut Notificator<T>)>
Operator<T, D1, D2, L> {
    pub fn new(receiver: PullCounter<T, D1>,
               targets:  Tee<T, D2>,
               name:     String,
               logic:    L,
               notify:   Option<(Vec<T>, usize)>)
           -> Operator<T, D1, D2, L> {

        Operator {
            name:    name,
            input:       receiver,
            output:      PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            notificator: Notificator::new(),
            logic:   logic,
            notify:  notify,
            internal_changes: Rc::new(RefCell::new(CountMap::new())),
        }
    }
}

impl<T, D1, D2, L> Operate<T> for Operator<T, D1, D2, L>
where T: Timestamp,
      D1: Data, D2: Data,
      L: FnMut(&mut InputHandle<T, D1>,
               &mut OutputHandle<T, D2, Tee<T, D2>>,
               &mut Notificator<T>) {
    fn inputs(&self) -> usize { 1 }
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
        (vec![vec![Antichain::from_elem(Default::default())]], internal)
    }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>,
                                       frontier: &mut [CountMap<T>]) {
        self.notificator.update_frontier_from_cm(frontier);
    }

    fn push_external_progress(&mut self, external: &mut [CountMap<T>]) {
        self.notificator.update_frontier_from_cm(external);
    }

    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<T>],
                                         internal: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool
    {
        {
            let mut input_handle = new_input_handle(&mut self.input, self.internal_changes.clone());
            let mut output_handle = new_output_handle(&mut self.output);
            (self.logic)(&mut input_handle, &mut output_handle, &mut self.notificator);
        }

        self.output.cease();

        // extract what we know about progress from the input and output adapters.
        self.input.pull_progress(&mut consumed[0]);
        self.output.inner().pull_progress(&mut produced[0]);
        self.internal_changes.borrow_mut().drain_into(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { self.name.clone() }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
