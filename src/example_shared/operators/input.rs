use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Scope, Timestamp};
use progress::nested::subgraph::Source::{ScopeOutput};
use progress::count_map::CountMap;
use progress::timestamp::RootTimestamp;
use progress::nested::product::Product;

use communication::*;
use communication::channels::ObserverHelper;

use example_shared::stream::Stream;
use example_shared::builder::*;

// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

// returns both an input scope and a stream representing its output.
pub trait InputExtensionTrait<C: Communicator, T: Timestamp+Ord> {
    fn new_input<D:Data>(&self) -> (InputHelper<T, D>, Stream<SubgraphBuilder<GraphRoot<C>, T>, D>);
}

impl<C: Communicator, T: Timestamp+Ord> InputExtensionTrait<C, T> for SubgraphBuilder<GraphRoot<C>, T> {
    fn new_input<D:Data>(&self) -> (InputHelper<T, D>, Stream<SubgraphBuilder<GraphRoot<C>, T>, D>) {

        let (output, registrar) = OutputPort::<Product<RootTimestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));

        let helper = InputHelper {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(CountMap::new())),
            output:   ObserverHelper::new(output, produced.clone()),
            now_at:   Default::default(),
            closed:   false,
        };

        let copies = self.peers();
        // println!("peers: {}", self.peers());

        let index = self.add_scope(InputScope {
            frontier: helper.frontier.clone(),
            progress: helper.progress.clone(),
            messages: produced.clone(),
            copies:   copies,
        });

        return (helper, Stream::new(ScopeOutput(index, 0), registrar, self.clone()));
    }
}

pub struct InputScope<T:Timestamp+Ord> {
    frontier:   Rc<RefCell<MutableAntichain<Product<RootTimestamp, T>>>>,   // times available for sending
    progress:   Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // times closed since last asked
    messages:   Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // messages sent since last asked
    copies:     u64,
}

impl<T:Timestamp+Ord> Scope<Product<RootTimestamp, T>> for InputScope<T> {
    fn name(&self) -> String { format!("Input") }
    fn inputs(&self) -> u64 { 0 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<Product<RootTimestamp, T> as Timestamp>::Summary>>>,
                                           Vec<CountMap<Product<RootTimestamp, T>>>) {
        let mut map = CountMap::new();
        for x in self.frontier.borrow().elements().iter() {
            map.update(x, self.copies as i64);
        }
        (Vec::new(), vec![map])
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut [CountMap<Product<RootTimestamp, T>>],
                                        _messages_consumed: &mut [CountMap<Product<RootTimestamp, T>>],
                                         messages_produced: &mut [CountMap<Product<RootTimestamp, T>>]) -> bool
    {
        self.messages.borrow_mut().drain_into(&mut messages_produced[0]);
        self.progress.borrow_mut().drain_into(&mut frontier_progress[0]);
        return false;
    }

    fn notify_me(&self) -> bool { false }
}

pub struct InputHelper<T: Timestamp+Ord, D: Data> {
    frontier:   Rc<RefCell<MutableAntichain<Product<RootTimestamp, T>>>>,   // times available for sending
    progress:   Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // times closed since last asked
    output:     ObserverHelper<OutputPort<Product<RootTimestamp, T>, D>>,

    now_at:     T,
    closed:     bool,
}

impl<T:Timestamp+Ord, D: Data> InputHelper<T, D> {
    pub fn send_at<I: Iterator<Item=D>>(&mut self, time: T, items: I) {
        if time >= self.now_at {
            self.output.open(&Product::new(RootTimestamp, time));
            for item in items { self.output.give(item); }
            self.output.shut(&Product::new(RootTimestamp, time));
        }
    }

    pub fn advance_to(&mut self, next: T) {
        if next > self.now_at {
            self.frontier.borrow_mut().update_weight(&Product::new(RootTimestamp, self.now_at.clone()), -1, &mut (*self.progress.borrow_mut()));
            self.frontier.borrow_mut().update_weight(&Product::new(RootTimestamp, next.clone()),  1, &mut (*self.progress.borrow_mut()));
            self.now_at = next;
        }
    }

    pub fn close(self) { }
}

impl<T:Timestamp+Ord, D: Data> Drop for InputHelper<T, D> {
    fn drop(&mut self) {
        // println!("dropping input");
        self.frontier.borrow_mut().update_weight(&Product::new(RootTimestamp, self.now_at.clone()), -1, &mut (*self.progress.borrow_mut()));
    }
}
