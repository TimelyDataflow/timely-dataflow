//! Methods to construct generic streaming and blocking unary operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::nested::subgraph::Source::ScopeOutput;
use progress::nested::subgraph::Target::ScopeInput;

use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Scope, Antichain};
use communication::observer::Tee;
use communication::observer::Counter as ObserverCounter;
use communication::observer::buffer::Buffer as ObserverBuffer;
use communication::pact::ParallelizationContract;
use communication::pullable::Counter as PullableCounter;
use communication::{Data, Pullable};

use construction::{Stream, GraphBuilder};

use drain::DrainExt;

type Input<T, D, P> = PullableCounter<T, D, P>;
type Output<T, D> = ObserverBuffer<ObserverCounter<Tee<T, D>>>;

/// Methods to construct generic streaming and blocking unary operators.

pub trait Unary<G: GraphBuilder, D1: Data> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream and
    /// write to the output stream.
    ///
    /// #Examples
    /// ```
    /// use communication::pact::Pipeline;
    /// source.unary_stream(Pipeline, "example", |input, output| {
    ///     while let Some((time, data)) = input.pul() {
    ///         output.session(time).give_message(data);
    ///     }
    /// });
    /// ```
    fn unary_stream<D2: Data,
             L: FnMut(&mut PullableCounter<G::Timestamp, D1, P::Pullable>,
                      &mut ObserverBuffer<ObserverCounter<Tee<G::Timestamp, D2>>>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
            (&self, pact: P, name: &str, logic: L) -> Stream<G, D2>;
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream,
    /// write to the output stream, and request and receive notifications. The method also requires
    /// a vector of initial notifications the operator requires (commonly none).
    ///
    /// #Examples
    /// ```
    /// use communication::pact::Pipeline;
    /// source.unary_notify(Pipeline, "example", vec![], |input, output, notificator| {
    ///     while let Some((time, data)) = input.pul() {
    ///         output.session(time).give_message(data);
    ///     }
    /// }
    /// ```
    fn unary_notify<D2: Data,
            L: FnMut(&mut PullableCounter<G::Timestamp, D1, P::Pullable>,
                     &mut ObserverBuffer<ObserverCounter<Tee<G::Timestamp, D2>>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
            (&self, pact: P, name: &str, init: Vec<G::Timestamp>, logic: L) -> Stream<G, D2>;

}

impl<G: GraphBuilder, D1: Data> Unary<G, D1> for Stream<G, D1> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut PullableCounter<G::Timestamp, D1, P::Pullable>,
                     &mut ObserverBuffer<ObserverCounter<Tee<G::Timestamp, D2>>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, init: Vec<G::Timestamp>, logic: L) -> Stream<G, D2> {

        let mut builder = self.builder();   // clones the builder

        let (sender, receiver) = pact.connect(&mut builder);
        let (targets, registrar) = Tee::<G::Timestamp,D2>::new();
        let scope = UnaryScope::new(receiver, targets, name.to_owned(), logic, Some((init, builder.peers())));
        let index = builder.add_scope(scope);

        self.connect_to(ScopeInput(index, 0), sender);

        Stream::new(ScopeOutput(index, 0), registrar, builder)
    }

    fn unary_stream<D2: Data,
             L: FnMut(&mut PullableCounter<G::Timestamp, D1, P::Pullable>,
                      &mut ObserverBuffer<ObserverCounter<Tee<G::Timestamp, D2>>>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, mut logic: L) -> Stream<G, D2> {

        let mut builder = self.builder();

        let (sender, receiver) = pact.connect(&mut builder);
        let (targets, registrar) = Tee::<G::Timestamp,D2>::new();
        let scope = UnaryScope::new(receiver, targets, name.to_owned(), move |i,o,_| logic(i,o), None);
        let index = builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);

        Stream::new(ScopeOutput(index, 0), registrar, builder)
    }
}

struct UnaryScopeHandle<T: Timestamp, D1: Data, D2: Data, P: Pullable<T, D1>> {
    pub input:          PullableCounter<T, D1, P>,
    pub output:         ObserverBuffer<ObserverCounter<Tee<T, D2>>>,
    pub notificator:    Notificator<T>,
}

struct UnaryScope
    <
    T: Timestamp,
    D1: Data,
    D2: Data,
    P: Pullable<T, D1>,
    L: FnMut(&mut PullableCounter<T, D1, P>,
             &mut ObserverBuffer<ObserverCounter<Tee<T, D2>>>,
             &mut Notificator<T>)> {
    name:           String,
    handle:         UnaryScopeHandle<T, D1, D2, P>,
    logic:          L,
    notify:         Option<(Vec<T>, u64)>,    // initial notifications and peers
}

impl<T:  Timestamp,
     D1: Data,
     D2: Data,
     P:  Pullable<T, D1>,
     L:  FnMut(&mut PullableCounter<T, D1, P>,
               &mut ObserverBuffer<ObserverCounter<Tee<T, D2>>>,
               &mut Notificator<T>)>
UnaryScope<T, D1, D2, P, L> {
    pub fn new(receiver: P,
               targets:  Tee<T, D2>,
               name:     String,
               logic:    L,
               notify:   Option<(Vec<T>, u64)>)
           -> UnaryScope<T, D1, D2, P, L> {
        UnaryScope {
            name:    name,
            handle:  UnaryScopeHandle {
                input:       PullableCounter::new(receiver),
                output:      ObserverBuffer::new(ObserverCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
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
      P: Pullable<T, D1>,
      L: FnMut(&mut PullableCounter<T, D1, P>,
               &mut ObserverBuffer<ObserverCounter<Tee<T, D2>>>,
               &mut Notificator<T>) {
    fn inputs(&self) -> u64 { 1 }
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

        self.handle.output.flush_and_shut();

        // extract what we know about progress from the input and output adapters.
        self.handle.input.pull_progress(&mut consumed[0]);
        self.handle.output.inner().pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("{}", self.name) }
    fn notify_me(&self) -> bool { self.notify.is_some() }
}
