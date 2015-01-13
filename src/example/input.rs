use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Graph, Scope, PathSummary, Timestamp};
use progress::graph::GraphExtension;
use progress::subgraph::Source::{ScopeOutput};
use progress::count_map::CountMap;
use communication::ChannelAllocator;

use communication::Observer;
use communication::channels::{Data, OutputPort, ObserverHelper};
use example::stream::Stream;

pub trait InputExtensionTrait<T: Timestamp, S: PathSummary<T>>
{
    // returns both an input scope and a stream representing its output.
    fn new_input<D:Data>(&mut self, allocator: Rc<RefCell<ChannelAllocator>>) -> (InputHelper<T, D>, Stream<T, S, D>);
}

impl<T: Timestamp, S: PathSummary<T>, G: Graph<T, S>> InputExtensionTrait<T, S> for G
{
    fn new_input<D:Data>(&mut self, allocator: Rc<RefCell<ChannelAllocator>>) -> (InputHelper<T, D>, Stream<T, S, D>) {
        let targets: Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>> = Rc::new(RefCell::new(Vec::new()));//Default::default();
        let produced = Rc::new(RefCell::new(Vec::new()));//Default::default();

        let helper = InputHelper {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(Vec::new())),
            output:   ObserverHelper::new(OutputPort{ shared: targets.clone() }, produced.clone()),
        };

        let index = self.add_scope(InputScope {
            frontier: helper.frontier.clone(),
            progress: helper.progress.clone(),
            messages: produced.clone(),
            copies:   allocator.borrow().multiplicity(),
        });

        return (helper, Stream {
            name: ScopeOutput(index, 0),
            ports: targets,
            graph: self.as_box(),
            allocator: allocator.clone()
        });
    }
}

pub struct InputScope<T:Timestamp> {
    frontier:   Rc<RefCell<MutableAntichain<T>>>,    // times available for sending
    progress:   Rc<RefCell<Vec<(T, i64)>>>,          // times closed since last asked
    messages:   Rc<RefCell<Vec<(T, i64)>>>,          // messages sent since last asked
    copies:     u64,
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for InputScope<T> {
    fn name(&self) -> String { format!("Input") }
    fn inputs(&self) -> u64 { 0 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>) {
        (Vec::new(), vec![self.frontier.borrow().elements.iter().map(|x| (x.clone(), self.copies as i64)).collect()])
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                        _messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                         messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for (ref key, val) in self.messages.borrow_mut().drain() { messages_produced[0].update(key, val); }
        for (ref key, val) in self.progress.borrow_mut().drain() { frontier_progress[0].update(key, val); }
        return false;
    }

    fn notify_me(&self) -> bool { false }
}

pub struct InputHelper<T: Timestamp, D: Data> {
    frontier:   Rc<RefCell<MutableAntichain<T>>>,    // times available for sending
    progress:   Rc<RefCell<Vec<(T, i64)>>>,          // times closed since last asked
    output:     ObserverHelper<OutputPort<T, D>>,
}

impl<T:Timestamp, D: Data> InputHelper<T, D> {
    pub fn send_messages(&mut self, time: &T, data: Vec<D>) {
        self.output.open(time);
        for datum in data.into_iter() { self.output.push(&datum); }
        self.output.shut(time);
    }

    pub fn advance(&self, start: &T, end: &T) {
        self.frontier.borrow_mut().update_weight(start, -1, &mut (*self.progress.borrow_mut()));
        self.frontier.borrow_mut().update_weight(end,  1, &mut (*self.progress.borrow_mut()));
    }

    pub fn close_at(&self, time: &T) {
        self.frontier.borrow_mut().update_weight(time, -1, &mut (*self.progress.borrow_mut()));
    }
}
