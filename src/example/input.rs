use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Graph, Scope, PathSummary, Timestamp};
use progress::subgraph::Source::{ScopeOutput};
use progress::count_map::CountMap;
use communication::ChannelAllocator;

use example::ports::{TargetPort, TeePort};
use example::stream::Stream;

pub trait InputExtensionTrait<T: Timestamp, S: PathSummary<T>>
{
    // returns both an input scope and a stream representing its output.
    fn new_input<D:Copy+'static>(&mut self, allocator: Rc<RefCell<ChannelAllocator>>) -> (InputHelper<T, D>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, G: Graph<T, S>> InputExtensionTrait<T, S> for G
{
    fn new_input<D:Copy+'static>(&mut self, allocator: Rc<RefCell<ChannelAllocator>>) -> (InputHelper<T, D>, Stream<T, S, D>)
    {
        let helper = InputHelper
        {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(Vec::new())),
            tee_port: TeePort::new(),
        };

        let index = self.add_scope(box InputScope
        {
            frontier: helper.frontier.clone(),
            progress: helper.progress.clone(),
            messages: helper.tee_port.updates.clone(),
            copies:   allocator.borrow().multiplicity,
        });

        let targets = helper.tee_port.targets.clone();
        return (helper, Stream{ name: ScopeOutput(index, 0), port: targets, graph: self.as_box(), allocator: allocator.clone() });
    }
}

pub struct InputScope<T:Timestamp>
{
    frontier:   Rc<RefCell<MutableAntichain<T>>>,    // times available for sending
    progress:   Rc<RefCell<Vec<(T, i64)>>>,          // times closed since last asked
    messages:   Rc<RefCell<Vec<(T, i64)>>>,          // messages sent since last asked
    copies:     uint,
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for InputScope<T>
{
    fn name(&self) -> String { format!("Input") }
    fn inputs(&self) -> uint { 0 }
    fn outputs(&self) -> uint { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>)
    {
        (Vec::new(), vec![self.frontier.borrow().elements.iter().map(|&x| (x, self.copies as i64)).collect()])
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                        _messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                         messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for &(key, val) in self.messages.borrow().iter() { messages_produced[0].push((key, val)); }
        self.messages.borrow_mut().clear();

        for &(key, val) in self.progress.borrow().iter() { frontier_progress[0].push((key, val)); }
        self.progress.borrow_mut().clear();

        return true;
    }

    fn notify_me(&self) -> bool { false }
}

pub struct InputHelper<T: Timestamp, D: Copy+'static>
{
    frontier:   Rc<RefCell<MutableAntichain<T>>>,    // times available for sending
    progress:   Rc<RefCell<Vec<(T, i64)>>>,          // times closed since last asked
    tee_port:   TeePort<T, D>,
}

impl<T:Timestamp, D: Copy+'static> InputHelper<T, D>
{
    pub fn send_messages(&self, time: &T, data: &Vec<D>)
    {
        self.tee_port.deliver_data(time, data);
    }

    pub fn advance(&self, start: &T, end: &T)
    {
        self.frontier.borrow_mut().update_weight(start, -1, &mut (*self.progress.borrow_mut()));
        self.frontier.borrow_mut().update_weight(end,  1, &mut (*self.progress.borrow_mut()));
    }

    pub fn close_at(&self, time: &T)
    {
        self.frontier.borrow_mut().update_weight(time, -1, &mut (*self.progress.borrow_mut()));
    }
}
