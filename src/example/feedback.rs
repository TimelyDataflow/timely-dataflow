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
    fn feedback(&mut self, limit: T, summary: S) -> (FeedbackHelper<T, S, D, ObserverHelper<T, D, FeedbackObserver<T, S, D>>>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, D:Data> FeedbackExtensionTrait<T, S, D> for Stream<T, S, D> {
    fn feedback(&mut self, limit: T, summary: S) -> (FeedbackHelper<T, S, D, ObserverHelper<T, D, FeedbackObserver<T, S, D>>>, Stream<T, S, D>) {

        let targets: Rc<RefCell<Vec<_>>>  = Default::default();
        let produced: Rc<RefCell<Vec<_>>> = Default::default();
        let consumed: Rc<RefCell<Vec<_>>> = Default::default();

        let feedback_output = ObserverHelper::new(OutputPort { shared: targets.clone() }, produced.clone());
        let feedback_input =  ObserverHelper::new(FeedbackObserver { limit: limit, summary: summary, targets: feedback_output }, consumed.clone());

        let index = self.graph.add_scope(FeedbackScope {
            consumed_messages:  consumed.clone(),
            produced_messages:  produced.clone(),
            summary:            summary,
        });

        let helper = FeedbackHelper {
            index:  index,
            target: Some(feedback_input),
        };

        return (helper, self.copy_with(ScopeOutput(index, 0), targets));
    }
}

// implementation of the feedback vertex, essentially, as an observer
pub struct FeedbackObserver<T: Timestamp, S: PathSummary<T>, D:Data> {
    limit:      T,
    summary:    S,
    targets:    ObserverHelper<T, D, OutputPort<T, D>>,
    // consumed:   Rc<RefCell<Vec<(T, i64)>>>,
}

impl<T: Timestamp, S: PathSummary<T>, D: Data> Observer<T, D> for FeedbackObserver<T, S, D> {
    fn open(&mut self, time: &T) { self.targets.open(&self.summary.results_in(time)); }
    fn push(&mut self, data: &D) { self.targets.push(data); } // TODO : Consult self.limit! buffer! break cycles!
    fn shut(&mut self, time: &T) { self.targets.shut(&self.summary.results_in(time)); }
}


// a handy widget for connecting feedback edges
pub struct FeedbackHelper<T:Timestamp, S: PathSummary<T>, D:Data, O: Observer<T, D>> {
    index:  u64,
    target: Option<O>,
}

impl<T:Timestamp, S:PathSummary<T>, D:Data, O: Observer<T, D>> FeedbackHelper<T, S, D, O> {
    pub fn connect_input(&mut self, source: &mut Stream<T, S, D>) -> () {
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
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for &(key, val) in self.consumed_messages.borrow().iter() { messages_consumed[0].push((key, val)); }
        for &(key, val) in self.produced_messages.borrow().iter() { messages_produced[0].push((key, val)); }

        self.consumed_messages.borrow_mut().clear();
        self.produced_messages.borrow_mut().clear();

        return false;
    }

    fn notify_me(&self) -> bool { false }
}
