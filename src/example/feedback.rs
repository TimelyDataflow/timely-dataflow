use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::frontier::Antichain;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use example::ports::{SourcePort, TargetPort};
use example::stream::Stream;

pub trait FeedbackExtensionTrait<T: Timestamp, S: PathSummary<T>, D:Copy+'static>
{
    fn feedback(&mut self, summary: S) -> (Rc<RefCell<FeedbackScope<T, S, D>>>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, D:Copy+'static> FeedbackExtensionTrait<T, S, D> for Stream<T, S, D>
{
    fn feedback(&mut self, summary: S) -> (Rc<RefCell<FeedbackScope<T, S, D>>>, Stream<T, S, D>)
    {
        let feedback = Rc::new(RefCell::new(FeedbackScope
        {
            index: 0,
            connected: false,
            consumed_messages: Vec::new(),
            produced_messages: Vec::new(),
            summary: summary,
            targets: Vec::new(),
        }));

        let index = self.graph.add_scope(box feedback.clone());
        feedback.borrow_mut().index = index;

        return (feedback.clone(), Stream { name: ScopeOutput(index, 0), port: box feedback, graph: self.graph.as_box() });
    }
}

pub struct FeedbackScope<T:Timestamp, S: PathSummary<T>, D:Copy+'static>
{
    index:              uint,
    connected:          bool,
    consumed_messages:  Vec<(T, i64)>,
    produced_messages:  Vec<(T, i64)>,
    summary:            S,

    targets:            Vec<Box<TargetPort<T, D>>>, // places to send things
}

impl<T:Timestamp, S:PathSummary<T>, D:Copy+'static> Scope<T, S> for Rc<RefCell<FeedbackScope<T, S, D>>>
{
    fn name(&self) -> String { format!("Feedback({})", self.borrow().index) }
    fn inputs(&self) -> uint { 1 }
    fn outputs(&self) -> uint { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>)
    {
        if !self.borrow().connected { println!("Feedback disconnected"); }

        (vec![vec![Antichain::from_elem(self.borrow().summary)]], vec![Vec::new()])
    }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> ()
    {
        let mut feedback = self.borrow_mut();

        for &(key, val) in feedback.consumed_messages.iter() { messages_consumed[0].push((key, val)); }
        for &(key, val) in feedback.produced_messages.iter() { messages_produced[0].push((key, val)); }

        feedback.consumed_messages.clear();
        feedback.produced_messages.clear();
    }

    fn notify_me(&self) -> bool { false }
}

impl<T:Timestamp, S:PathSummary<T>, D:Copy+'static> SourcePort<T, D> for Rc<RefCell<FeedbackScope<T, S, D>>>
{
    fn register_interest(&mut self, target: Box<TargetPort<T, D>>) -> ()
    {
        self.borrow_mut().targets.push(target);
    }
}

impl<T:Timestamp, S:PathSummary<T>, D:Copy+'static> TargetPort<T, D> for Rc<RefCell<FeedbackScope<T, S, D>>>
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>)
    {
        let mut feedback = self.borrow_mut();
        let new_time = feedback.summary.results_in(time);

        feedback.consumed_messages.update(*time, 1);
        feedback.produced_messages.update(new_time, 1);

        for target in feedback.targets.iter_mut()
        {
            target.deliver_data(&new_time, data);
        }
    }
}

impl<T:Timestamp, S:PathSummary<T>, D:Copy+'static> FeedbackScope<T, S, D>
{
    pub fn connect_input<G: Graph<T, S>>(_graph: &mut G, feedback: Rc<RefCell<FeedbackScope<T, S, D>>>, source: &mut Stream<T, S, D>) -> ()
    {
        let connected = feedback.borrow().connected;

        if connected
        {
            println!("Feedback already connected")
        }
        else
        {
            let mut borrow = feedback.borrow_mut();

            borrow.connected = true;
            source.graph.connect(source.name, ScopeInput(borrow.index, 0));

            source.port.register_interest(box feedback.clone());
        }
    }
}
