use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Graph, Scope, PathSummary, Timestamp};
use progress::subgraph::Source::{ScopeOutput};
use progress::count_map::CountMap;

use example::ports::TargetPort;
use example::tee::new_tee;
use example::stream::Stream;


pub trait InputExtensionTrait<T: Timestamp, S: PathSummary<T>>
{
    // returns both an input scope and a stream representing its output.
    fn new_input<D:Copy+'static>(&mut self) -> (Rc<RefCell<InputScope<T>>>, Box<TargetPort<T, D>>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, G: Graph<T, S>> InputExtensionTrait<T, S> for G
{
    fn new_input<D:Copy+'static>(&mut self) -> (Rc<RefCell<InputScope<T>>>, Box<TargetPort<T, D>>, Stream<T, S, D>)
    {
        let input = Rc::new(RefCell::new(InputScope
        {
            frontier: MutableAntichain::new_bottom(Default::default()),
            progress: Vec::new(),
            messages: Vec::new(),
        }));

        let index = self.add_scope(box input.clone());

        let (source, target) = new_tee(ScopeOutput(index, 0));

        return (input, target, Stream{ name: ScopeOutput(index, 0), port: source, graph: self.as_box() });
    }
}

pub struct InputScope<T:Timestamp>
{
    frontier:   MutableAntichain<T>,    // times available for sending
    progress:   Vec<(T, i64)>,          // times closed since last asked
    messages:   Vec<(T, i64)>,          // messages sent since last asked
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for Rc<RefCell<InputScope<T>>>
{
    fn name(&self) -> String { format!("Input") }
    fn inputs(&self) -> uint { 0 }
    fn outputs(&self) -> uint { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>)
    {
        return (Vec::new(), Vec::from_fn(1, |_| { self.borrow().frontier.elements.iter().map(|&x| (x, 1)).collect() }));
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                        _messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                         messages_produced: &mut Vec<Vec<(T, i64)>>) -> ()
    {
        let mut input = self.borrow_mut();

        for &(key, val) in input.messages.iter() { messages_produced[0].push((key, val)); }
        input.messages.clear();

        for &(key, val) in input.progress.iter() { frontier_progress[0].push((key, val)); }
        input.progress.clear();
    }

    fn notify_me(&self) -> bool { false }
}

impl<T:Timestamp> InputScope<T>
{
    pub fn sent_messages(&mut self, time: T, count: i64, next: T)
    {
        self.frontier.update_weight(&time, -1, &mut self.progress);
        self.frontier.update_weight(&next,  1, &mut self.progress);

        self.messages.update(time, count);
    }
}
