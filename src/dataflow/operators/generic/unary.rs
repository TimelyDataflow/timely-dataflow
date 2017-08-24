//! Methods to construct generic streaming and blocking unary operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use timely_communication::Pull;
use dataflow::channels::Content;

use progress::nested::subgraph::{Source, Target};

use progress::ChangeBatch;
use dataflow::operators::Notificator;
use progress::{Timestamp, Operate, Antichain};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pullers::Counter as PullCounter;

use dataflow::operators::generic::handles::{InputHandle, OutputHandle};
use dataflow::operators::generic::handles::{access_pull_counter, new_input_handle, new_output_handle};
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
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                input.for_each(|time, data| {
    ///                    output.session(&time).give_content(data);
    ///                });
    ///            });
    /// });
    /// ```
    fn unary_stream<D2, L, P> (&self, pact: P, name: &str, logic: L) -> Stream<G, D2>
    where
        D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream,
    /// write to the output stream, and request and receive notifications. The method also requires
    /// a vector of the initial notifications the operator requires (commonly none).
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                input.for_each(|time, data| {
    ///                    output.session(&time).give_content(data);
    ///                    notificator.notify_at(time);
    ///                });
    ///                notificator.for_each(|time,_,_| {
    ///                    println!("done with time: {:?}", time.time());
    ///                });
    ///            });
    /// });
    /// ```
    fn unary_notify<D2, L, P>(&self, pact: P, name: &str, init: Vec<G::Timestamp>, logic: L) -> Stream<G, D2>
    where
        D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                 &mut Notificator<G::Timestamp>)+'static,
         P: ParallelizationContract<G::Timestamp, D1>;
}

impl<G: Scope, D1: Data> Unary<G, D1> for Stream<G, D1> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
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
             L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
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
    P: Pull<(T, Content<D1>)>,
    L: FnMut(&mut InputHandle<T, D1, P>,
             &mut OutputHandle<T, D2, Tee<T, D2>>,
             &mut Notificator<T>)> {
    name:             String,
    input:            InputHandle<T, D1, P>,
    output:           PushBuffer<T, D2, PushCounter<T, D2, Tee<T, D2>>>,
    notificator:      Notificator<T>,
    logic:            L,
    notify:           Option<(Vec<T>, usize)>,    // initial notifications and peers
    internal_changes: Rc<RefCell<ChangeBatch<T>>>,
}

impl<T:  Timestamp,
     D1: Data,
     D2: Data,
     P: Pull<(T, Content<D1>)>,
     L:  FnMut(&mut InputHandle<T, D1, P>,
               &mut OutputHandle<T, D2, Tee<T, D2>>,
               &mut Notificator<T>)>
Operator<T, D1, D2, P, L> {
    pub fn new(receiver: PullCounter<T, D1, P>,
               targets:  Tee<T, D2>,
               name:     String,
               logic:    L,
               notify:   Option<(Vec<T>, usize)>)
           -> Operator<T, D1, D2, P, L> {

        let internal = Rc::new(RefCell::new(ChangeBatch::new()));
        Operator {
            name:    name,
            input:       new_input_handle(receiver, internal.clone()),
            output:      PushBuffer::new(PushCounter::new(targets)),
            notificator: Notificator::new(),
            logic:   logic,
            notify:  notify,
            internal_changes: internal,
        }
    }
}

impl<T, D1, D2, P, L> Operate<T> for Operator<T, D1, D2, P, L>
where T: Timestamp,
      D1: Data, 
      D2: Data,
      P: Pull<(T, Content<D1>)>,
      L: FnMut(&mut InputHandle<T, D1, P>,
               &mut OutputHandle<T, D2, Tee<T, D2>>,
               &mut Notificator<T>) {
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {
        // by the end of the method, we want the return Vec<ChangeBatch<T>> to contain each reserved capability, 
        // *multiplied by the number of peers*. This is so that each worker knows to wait for each other worker
        // instance. Importantly, we do not want to multiply the number of capabilities by the number of peers.

        let mut internal = vec![ChangeBatch::new()];
        if let Some((ref mut initial, peers)) = self.notify {
            for time in initial.drain(..) {
                self.notificator.notify_at(mint_capability(time, self.internal_changes.clone()));
            }

            // augment the counts for each reserved capability.
            for &(ref time, count) in self.internal_changes.borrow_mut().iter() {
                internal[0].update(time.clone(), count * (peers as i64 - 1));
            }

            // drain the changes to empty out, and complete the counts for internal.
            self.internal_changes.borrow_mut().drain_into(&mut internal[0]);
        }
        (vec![vec![Antichain::from_elem(Default::default())]], internal)
    }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>,
                                       frontier: &mut [ChangeBatch<T>]) {
        self.notificator.update_frontier_from_cm(frontier);
    }

    fn push_external_progress(&mut self, external: &mut [ChangeBatch<T>]) {
        self.notificator.update_frontier_from_cm(external);
    }

    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<T>],
                                         internal: &mut [ChangeBatch<T>],
                                         produced: &mut [ChangeBatch<T>]) -> bool
    {
        {
            let mut output_handle = new_output_handle(&mut self.output);
            (self.logic)(&mut self.input, &mut output_handle, &mut self.notificator);
        }

        // extract what we know about progress from the input and output adapters.
        access_pull_counter(&mut self.input).consumed().borrow_mut().drain_into(&mut consumed[0]);
        self.output.inner().produced().borrow_mut().drain_into(&mut produced[0]);
        self.internal_changes.borrow_mut().drain_into(&mut internal[0]);
        
        false   // no unannounced internal work
    }

    fn name(&self) -> String { self.name.clone() }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
