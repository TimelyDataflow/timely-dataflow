use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Graph, Scope, Timestamp};
use progress::graph::GraphExtension;
use progress::subgraph::Source::{ScopeOutput};
use progress::count_map::CountMap;
use communication::Communicator;

use communication::Observer;
use communication::channels::{Data, OutputPort, ObserverHelper};
use example::stream::Stream;

// returns both an input scope and a stream representing its output.
pub trait InputExtensionTrait<G: Graph> {
    fn new_input<D:Data>(&mut self, allocator: Rc<RefCell<Communicator>>) -> (InputHelper<G::Timestamp, D>, Stream<G, D>);
}

impl<G: Graph> InputExtensionTrait<G> for G {
    fn new_input<D:Data>(&mut self, allocator: Rc<RefCell<Communicator>>) -> (InputHelper<G::Timestamp, D>, Stream<G, D>) {
        let output: OutputPort<G::Timestamp, D> = Default::default();
        let produced = Rc::new(RefCell::new(CountMap::new()));

        let helper = InputHelper {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(CountMap::new())),
            output:   ObserverHelper::new(output.clone(), produced.clone()),
        };

        let index = self.add_scope(InputScope {
            frontier: helper.frontier.clone(),
            progress: helper.progress.clone(),
            messages: produced.clone(),
            copies:   allocator.borrow().peers(),
        });

        return (helper, Stream {
            name: ScopeOutput(index, 0),
            ports: output,
            graph: self.clone(),
            allocator: allocator.clone()
        });
    }
}

pub struct InputScope<T:Timestamp> {
    frontier:   Rc<RefCell<MutableAntichain<T>>>,    // times available for sending
    progress:   Rc<RefCell<CountMap<T>>>,          // times closed since last asked
    messages:   Rc<RefCell<CountMap<T>>>,          // messages sent since last asked
    copies:     u64,
}

impl<T:Timestamp> Scope<T> for InputScope<T> {
    fn name(&self) -> String { format!("Input") }
    fn inputs(&self) -> u64 { 0 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        let mut map = CountMap::new();
        for x in self.frontier.borrow().elements.iter() {
            map.update(x, self.copies as i64);
        }
        (Vec::new(), vec![map])
        // (Vec::new(), vec![self.frontier.borrow().elements.iter().map(|x| (x.clone(), self.copies as i64)).collect()])
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<CountMap<T>>,
                                        _messages_consumed: &mut Vec<CountMap<T>>,
                                         messages_produced: &mut Vec<CountMap<T>>) -> bool
    {
        self.messages.borrow_mut().drain_into(&mut messages_produced[0]);
        self.progress.borrow_mut().drain_into(&mut frontier_progress[0]);
        // for (ref key, val) in self.messages.borrow_mut().drain() { messages_produced[0].update(key, val); }
        // for (ref key, val) in self.progress.borrow_mut().drain() { frontier_progress[0].update(key, val); }
        return false;
    }

    fn notify_me(&self) -> bool { false }
}

pub struct InputHelper<T: Timestamp, D: Data> {
    frontier:   Rc<RefCell<MutableAntichain<T>>>,    // times available for sending
    progress:   Rc<RefCell<CountMap<T>>>,          // times closed since last asked
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
