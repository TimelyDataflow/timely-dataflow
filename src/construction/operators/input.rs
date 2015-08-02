use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Operate, Timestamp};
use progress::nested::subgraph::Source::ChildOutput;
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
pub trait Extension<C: Communicator, T: Timestamp+Ord> {
    fn new_input<D:Data>(&self) -> (InputHelper<T, D>, Stream<SubgraphBuilder<GraphRoot<C>, T>, D>);
}

impl<C: Communicator, T: Timestamp+Ord> Extension<C, T> for SubgraphBuilder<GraphRoot<C>, T> {
    fn new_input<D:Data>(&self) -> (InputHelper<T, D>, Stream<SubgraphBuilder<GraphRoot<C>, T>, D>) {

        let (output, registrar) = Tee::<Product<RootTimestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let helper = InputHelper::new(Counter::new(output, produced.clone()));
        let copies = self.peers();

        let index = self.add_operator(Operator {
            frontier: helper.frontier.clone(),
            progress: helper.progress.clone(),
            messages: produced.clone(),
            copies:   copies,
        });

        return (helper, Stream::new(ChildOutput(index, 0), registrar, self.clone()));
    }
}

pub struct Operator<T:Timestamp+Ord> {
    frontier:   Rc<RefCell<MutableAntichain<Product<RootTimestamp, T>>>>,   // times available for sending
    progress:   Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // times closed since last asked
    messages:   Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // messages sent since last asked
    copies:     usize,
}

impl<T:Timestamp+Ord> Operate<Product<RootTimestamp, T>> for Operator<T> {
    fn name(&self) -> &str { "Input" }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

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


/// Manages the movement of data into the dataflow from the outside world.
pub struct InputHelper<T: Timestamp+Ord, D: Data> {
    frontier: Rc<RefCell<MutableAntichain<Product<RootTimestamp, T>>>>,   // times available for sending
    progress: Rc<RefCell<CountMap<Product<RootTimestamp, T>>>>,           // times closed since last asked
    observer: Counter<Tee<Product<RootTimestamp, T>, D>>,
    buffer: Vec<D>,
    now_at: Option<Product<RootTimestamp, T>>,
}

// an input helper's state is either uninitialized, with now_at == None, or at some specific time.
// if now_at == None it has a hold on Default::default(), else it has a hold on the specific time.
// if now_at == None the observer has not been opened, else it is open with the specific time.


impl<T:Timestamp+Ord, D: Data> InputHelper<T, D> {
    fn new(observer: Counter<Tee<Product<RootTimestamp, T>, D>>) -> InputHelper<T, D> {
        InputHelper {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(CountMap::new())),
            observer: observer,
            buffer: Vec::with_capacity(Message::<D>::default_length()),
            now_at: None,
        }
    }

    // flushes any data we are sitting on. may need to initialize self.now_at if no one has yet.
    fn flush(&mut self) {
        // if we haven't yet we should initialize now_at, to Default::default() because
        // we haven't been told anything better.
        if self.now_at.is_none() {
            self.now_at = Some(Default::default());
            self.observer.open(&Default::default());
            // we skip the frontier update because we would be adding and subtracting from the same
            // time: Default::default().
        }

        let mut message = Message::from_typed(&mut self.buffer);
        self.observer.give(&mut message);
        self.buffer = message.into_typed();
        if self.buffer.capacity() != Message::<D>::default_length() {
            assert!(self.buffer.capacity() == 0);
            self.buffer = Vec::with_capacity(Message::<D>::default_length());
        }
    }

    // closes the current epoch, flushing if needed, shutting if needed, and updating the frontier.
    fn close_epoch(&mut self) {
        // if we are sitting on any data, clear it out.
        if self.buffer.len() > 0 { self.flush(); }

        // shut things down if needed; release hold.
        if let Some(old_time) = self.now_at {
            self.observer.shut(&old_time);
            self.frontier.borrow_mut().update_weight(&old_time, -1, &mut (*self.progress.borrow_mut()));
        }
        else {
            let old_time = Default::default();
            self.frontier.borrow_mut().update_weight(&old_time, -1, &mut (*self.progress.borrow_mut()));
        }
    }

    #[inline(always)]
    pub fn give(&mut self, data: D) {
        assert!(self.buffer.capacity() == Message::<D>::default_length());
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }

    pub fn advance_to(&mut self, next: T) {
        // attempting to reverse time's arrow is not supported.
        if let Some(time) = self.now_at { assert!(next >= time.inner); }

        self.close_epoch();

        // open up new epoch
        let new_time = Product::new(RootTimestamp, next);
        self.now_at = Some(new_time);
        self.observer.open(&new_time);
        self.frontier.borrow_mut().update_weight(&new_time,  1, &mut (*self.progress.borrow_mut()));
    }

    pub fn close(self) { }

    pub fn epoch(&mut self) -> &T {
        if self.now_at.is_none() {
            self.now_at = Some(Default::default());
            self.observer.open(&Default::default());
        }

        &self.now_at.as_ref().unwrap().inner
    }
}

impl<T:Timestamp+Ord, D: Data> Drop for InputHelper<T, D> {
    fn drop(&mut self) {
        self.close_epoch();
    }
}
