use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::frontier::Antichain;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::Observer;
use communication::channels::ObserverHelper;
use communication::channels::{Data, OutputPort};
use example::stream::Stream;


pub trait FeedbackExtensionTrait<G: Graph, D:Data> {
    fn feedback(&mut self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary) -> (FeedbackHelper<ObserverHelper<FeedbackObserver<G, D>>>, Stream<G, D>);
}

impl<G: Graph, D:Data> FeedbackExtensionTrait<G, D> for Stream<G, D> {
    fn feedback(&mut self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary) -> (FeedbackHelper<ObserverHelper<FeedbackObserver<G, D>>>, Stream<G, D>) {

        let targets: OutputPort<G::Timestamp, D> = Default::default();
        let produced: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();
        let consumed: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();

        let feedback_output = ObserverHelper::new(targets.clone(), produced.clone());
        let feedback_input =  ObserverHelper::new(FeedbackObserver { limit: limit, summary: summary, targets: feedback_output, active: false }, consumed.clone());

        let index = self.graph.add_scope(FeedbackScope {
            consumed_messages:  consumed.clone(),
            produced_messages:  produced.clone(),
            summary:            summary,
        });

        let helper = FeedbackHelper {
            index:  index,
            target: Some(feedback_input),
        };

        let result = Stream {
            name: ScopeOutput(index, 0),
            ports: targets,
            graph: self.graph.clone(),
            allocator: self.allocator.clone(),
        };

        return (helper, result);
    }
}

enum FeedbackObserverStatus<T: Timestamp> {
    Active(T),
    Inactive,
}

// implementation of the feedback vertex, essentially, as an observer
pub struct FeedbackObserver<G: Graph, D:Data> {
    limit:      G::Timestamp,
    summary:    <G::Timestamp as Timestamp>::Summary,
    targets:    ObserverHelper<OutputPort<G::Timestamp, D>>,
    active:     bool,
    // status:     FeedbackObserverStatus<T>,  // for debugging ideally
}

impl<G: Graph, D: Data> Observer for FeedbackObserver<G, D> {
    type Time = G::Timestamp;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &G::Timestamp) {
        self.active = time.le(&self.limit); // don't send if not less than limit
        // println!("active: {}", self.active);
        if self.active { self.targets.open(&self.summary.results_in(time)); }
    }
    #[inline(always)] fn push(&mut self, data: &D) { if self.active { self.targets.push(data); } }
    #[inline(always)] fn shut(&mut self, time: &G::Timestamp) { if self.active { self.targets.shut(&self.summary.results_in(time)); } }
}


// a handy widget for connecting feedback edges
pub struct FeedbackHelper<O: Observer> {
    index:  u64,
    target: Option<O>,
}

impl<O: Observer+'static> FeedbackHelper<O> where O::Time: Timestamp, O::Data : Data {
    pub fn connect_input<G:Graph<Timestamp=O::Time>>(&mut self, source: &mut Stream<G, O::Data>) -> () {
        source.graph.connect(source.name, ScopeInput(self.index, 0));
        source.add_observer(self.target.take().unwrap());
    }
}


// the scope that the progress tracker interacts with
pub struct FeedbackScope<T:Timestamp> {
    consumed_messages:  Rc<RefCell<CountMap<T>>>,
    produced_messages:  Rc<RefCell<CountMap<T>>>,
    summary:            T::Summary,
}

impl<T:Timestamp> Scope<T> for FeedbackScope<T> {
    fn name(&self) -> String { format!("Feedback") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        (vec![vec![Antichain::from_elem(self.summary)]], vec![CountMap::new()])
    }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<CountMap<T>>,
                                          messages_consumed: &mut Vec<CountMap<T>>,
                                          messages_produced: &mut Vec<CountMap<T>>) -> bool {

        self.consumed_messages.borrow_mut().drain_into(&mut messages_consumed[0]);
        self.produced_messages.borrow_mut().drain_into(&mut messages_produced[0]);
        // for (ref key, val) in self.consumed_messages.borrow_mut().drain() { messages_consumed[0].update(key, val); }
        // for (ref key, val) in self.produced_messages.borrow_mut().drain() { messages_produced[0].update(key, val); }
        // println!("feedback pulled: c: {}, p: {}", messages_consumed[0].len(), messages_produced[0].len());
        return false;
    }

    fn notify_me(&self) -> bool { false }
}
