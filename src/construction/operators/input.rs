use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Scope, Timestamp};
use progress::nested::subgraph::Source::{ScopeOutput};
use progress::count_map::CountMap;
use progress::timestamp::RootTimestamp;
use progress::nested::product::Product;

use communication::{Communicator, Data, Message, Observer};
use communication::observer::{Tee, Counter};

use construction::stream::Stream;
use construction::builder::*;

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

        let (output, registrar) = Tee::<Product<RootTimestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));

        let helper = InputHelper::new(Counter::new(output, produced.clone()));
        // {
        //     frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
        //     progress: Rc::new(RefCell::new(CountMap::new())),
        //     output:   Counter::new(output, produced.clone()),
        //     now_at:   Default::default(),
        //     closed:   false,
        // };

        let copies = self.peers();

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
    frontier: Rc<RefCell<MutableAntichain<Product<RootTimestamp, T>>>>,   // times available for sending
    progress: Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // times closed since last asked
    observer: Counter<Tee<Product<RootTimestamp, T>, D>>,
    buffer: Vec<D>,
    now_at: T,
}

impl<T:Timestamp+Ord, D: Data> InputHelper<T, D> {
    fn new(mut observer: Counter<Tee<Product<RootTimestamp, T>, D>>) -> InputHelper<T, D> {
        InputHelper {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(CountMap::new())),
            observer: observer,
            buffer: Vec::with_capacity(4096),
            now_at: Default::default(),
        }
    }

    fn flush(&mut self) {
        let mut message = Message::from_typed(&mut self.buffer);
        self.observer.give(&mut message);
        self.buffer = message.into_typed(4096);
        self.buffer.clear();
    }

    #[inline] pub fn open(&mut self) {
        self.observer.open(&Default::default());
    }

    #[inline(always)] pub fn give(&mut self, data: D) {
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }

    pub fn advance_to(&mut self, next: T) {
        if next > self.now_at {
            if self.buffer.len() > 0 {
                self.flush();
            }
            self.observer.shut(&Product::new(RootTimestamp, self.now_at.clone()));

            self.frontier.borrow_mut().update_weight(&Product::new(RootTimestamp, self.now_at.clone()), -1, &mut (*self.progress.borrow_mut()));
            self.frontier.borrow_mut().update_weight(&Product::new(RootTimestamp, next.clone()),  1, &mut (*self.progress.borrow_mut()));
            self.now_at = next;
            self.observer.open(&Product::new(RootTimestamp, self.now_at.clone()));
        }
        else {
            println!("attempted to advance to {:?}, but input already at {:?}", next, self.now_at);
        }
    }

    pub fn close(self) { }

    pub fn epoch(&self) -> &T { &self.now_at }
}

impl<T:Timestamp+Ord, D: Data> Drop for InputHelper<T, D> {
    fn drop(&mut self) {
        if self.buffer.len() > 0 {
            self.flush();
        }
        self.observer.shut(&Product::new(RootTimestamp, self.now_at.clone()));
        self.frontier.borrow_mut().update_weight(&Product::new(RootTimestamp, self.now_at.clone()), -1, &mut (*self.progress.borrow_mut()));
    }
}
