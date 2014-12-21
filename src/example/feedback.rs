use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::frontier::Antichain;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use example::ports::TargetPort;
use communication::channels::{Data, OutputPort};
use example::stream::Stream;

pub trait FeedbackExtensionTrait<T: Timestamp, S: PathSummary<T>, D:Data>
{
    fn feedback(&mut self, limit: T, summary: S) -> (FeedbackHelper<T, S, D>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, D:Data> FeedbackExtensionTrait<T, S, D> for Stream<T, S, D>
{
    fn feedback(&mut self, limit: T, summary: S) -> (FeedbackHelper<T, S, D>, Stream<T, S, D>)
    {
        let target_port = FeedbackTargetPort
        {
            limit:      limit,
            summary:    summary,
            targets:    OutputPort::new(),
            consumed:   Rc::new(RefCell::new(Vec::new())),
        };

        // capture these guys for later use
        let targets = target_port.targets.clone();

        let index = self.graph.add_scope(box FeedbackScope
        {
            consumed_messages:  target_port.consumed.clone(),
            produced_messages:  target_port.targets.updates.clone(),
            summary:            summary,
        });

        let helper = FeedbackHelper
        {
            index:  index,
            target: Some(box target_port as Box<TargetPort<T, D>>),
        };

        return (helper, self.copy_with(ScopeOutput(index, 0), targets.targets));
    }
}

pub struct FeedbackTargetPort<T: Timestamp, S: PathSummary<T>, D:Data>
{
    limit:      T,
    summary:    S,
    targets:    OutputPort<T, D>,
    consumed:   Rc<RefCell<Vec<(T, i64)>>>,
}

impl<T: Timestamp, S: PathSummary<T>, D: Data> TargetPort<T, D> for FeedbackTargetPort<T, S, D>
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>)
    {
        self.consumed.borrow_mut().update(*time, data.len() as i64);

        let new_time = self.summary.results_in(time);

        if new_time.le(&self.limit)
        {
            self.targets.deliver_data(&new_time, data);
        }
    }

    fn flush(&mut self) -> () { self.targets.flush(); }
}


pub struct FeedbackHelper<T:Timestamp, S: PathSummary<T>, D:Copy+'static>
{
    index:  uint,
    target: Option<Box<TargetPort<T, D>>>,
}

impl<T:Timestamp, S:PathSummary<T>, D:Copy+'static> FeedbackHelper<T, S, D>
{
    pub fn connect_input(&mut self, source: &mut Stream<T, S, D>) -> ()
    {
        source.graph.connect(source.name, ScopeInput(self.index, 0));
        source.port.borrow_mut().push(self.target.take().unwrap());
    }
}


pub struct FeedbackScope<T:Timestamp, S: PathSummary<T>>
{
    consumed_messages:  Rc<RefCell<Vec<(T, i64)>>>,
    produced_messages:  Rc<RefCell<Vec<(T, i64)>>>,
    summary:            S,
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for FeedbackScope<T, S>
{
    fn name(&self) -> String { format!("Feedback") }
    fn inputs(&self) -> uint { 1 }
    fn outputs(&self) -> uint { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>)
    {
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
