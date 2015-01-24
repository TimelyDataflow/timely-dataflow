use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::frontier::Antichain;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;
use progress::graph::GraphExtension;


use communication::Observer;
use communication::channels::ObserverHelper;
use communication::channels::{Data, OutputPort};
use example::stream::Stream;

pub trait FeedbackExtensionTrait<T: Timestamp, S: PathSummary<T>, D:Data> {
    fn feedback(&mut self, limit: T, summary: S) -> (FeedbackHelper<ObserverHelper<FeedbackObserver<T, S, D>>>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, D:Data> FeedbackExtensionTrait<T, S, D> for Stream<T, S, D> {
    fn feedback(&mut self, limit: T, summary: S) -> (FeedbackHelper<ObserverHelper<FeedbackObserver<T, S, D>>>, Stream<T, S, D>) {

        let targets: OutputPort<T, D> = Default::default();
        let produced: Rc<RefCell<Vec<(T, i64)>>> = Default::default();
        let consumed: Rc<RefCell<Vec<(T, i64)>>> = Default::default();

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
            graph: self.graph.as_box(),
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
pub struct FeedbackObserver<T: Timestamp, S: PathSummary<T>, D:Data> {
    limit:      T,
    summary:    S,
    targets:    ObserverHelper<OutputPort<T, D>>,
    active:     bool,
    // status:     FeedbackObserverStatus<T>,  // for debugging ideally
}

impl<T: Timestamp, S: PathSummary<T>, D: Data> Observer for FeedbackObserver<T, S, D> {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &T) {
        self.active = time.le(&self.limit); // don't send if not less than limit
        // println!("active: {}", self.active);
        if self.active { self.targets.open(&self.summary.results_in(time)); }
    }
    #[inline(always)] fn push(&mut self, data: &D) { if self.active { self.targets.push(data); } }
    #[inline(always)] fn shut(&mut self, time: &T) { if self.active { self.targets.shut(&self.summary.results_in(time)); } }
}


// a handy widget for connecting feedback edges
pub struct FeedbackHelper<O: Observer> {
    index:  u64,
    target: Option<O>,
}

impl<O: Observer> FeedbackHelper<O> where O::Time : Timestamp, O::Data : Data {
    pub fn connect_input<S: PathSummary<O::Time>>(&mut self, source: &mut Stream<O::Time, S, O::Data>) -> () {
        source.graph.connect(source.name, ScopeInput(self.index, 0));
        source.add_observer(self.target.take().unwrap());
    }
}


// the scope that the progress tracker interacts with
pub struct FeedbackScope<T:Timestamp, S: PathSummary<T>> {
    consumed_messages:  Rc<RefCell<Vec<(T, i64)>>>,
    produced_messages:  Rc<RefCell<Vec<(T, i64)>>>,
    summary:            S,
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for FeedbackScope<T, S> {
    fn name(&self) -> String { format!("Feedback") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>) {
        (vec![vec![Antichain::from_elem(self.summary)]], vec![Vec::new()])
    }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool {
        for (ref key, val) in self.consumed_messages.borrow_mut().drain() { messages_consumed[0].update(key, val); }
        for (ref key, val) in self.produced_messages.borrow_mut().drain() { messages_produced[0].update(key, val); }
        // println!("feedback pulled: c: {}, p: {}", messages_consumed[0].len(), messages_produced[0].len());
        return false;
    }

    fn notify_me(&self) -> bool { false }
}
