use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Operate, Timestamp};
use progress::nested::subgraph::Source::ChildOutput;
use progress::count_map::CountMap;
use progress::timestamp::RootTimestamp;
use progress::nested::product::Product;

use timely_communication::Allocate;
use {Data, Push};
use dataflow::channels::Content;
use dataflow::channels::pushers::{Tee, Counter};

use dataflow::{Stream, Scope};
use dataflow::scopes::{Child, Root};

// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

// returns both an input scope and a stream representing its output.
pub trait Input<A: Allocate, T: Timestamp+Ord> {
    fn new_input<D:Data>(&self) -> (InputHelper<T, D>, Stream<Child<Root<A>, T>, D>);
}

impl<A: Allocate, T: Timestamp+Ord> Input<A, T> for Child<Root<A>, T> {
    fn new_input<D:Data>(&self) -> (InputHelper<T, D>, Stream<Child<Root<A>, T>, D>) {

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

struct Operator<T:Timestamp+Ord> {
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
    pusher: Counter<Product<RootTimestamp, T>, D, Tee<Product<RootTimestamp, T>, D>>,
    buffer: Vec<D>,
    now_at: Product<RootTimestamp, T>,
}

// an input helper's state is either uninitialized, with now_at == None, or at some specific time.
// if now_at == None it has a hold on Default::default(), else it has a hold on the specific time.
// if now_at == None the pusher has not been opened, else it is open with the specific time.


impl<T:Timestamp+Ord, D: Data> InputHelper<T, D> {
    fn new(pusher: Counter<Product<RootTimestamp, T>, D, Tee<Product<RootTimestamp, T>, D>>) -> InputHelper<T, D> {
        InputHelper {
            frontier: Rc::new(RefCell::new(MutableAntichain::new_bottom(Default::default()))),
            progress: Rc::new(RefCell::new(CountMap::new())),
            pusher: pusher,
            buffer: Vec::with_capacity(Content::<D>::default_length()),
            now_at: Default::default(),
        }
    }

    // flushes any data we are sitting on. may need to initialize self.now_at if no one has yet.
    fn flush(&mut self) {
        Content::push_at(&mut self.buffer, self.now_at.clone(), &mut self.pusher);
    }

    // closes the current epoch, flushing if needed, shutting if needed, and updating the frontier.
    fn close_epoch(&mut self) {
        if self.buffer.len() > 0 { self.flush(); }
        self.pusher.done();
        self.frontier.borrow_mut().update_weight(&self.now_at, -1, &mut (*self.progress.borrow_mut()));
    }

    #[inline(always)]
    pub fn send(&mut self, data: D) {
        // assert!(self.buffer.capacity() == Content::<D>::default_length());
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }

    pub fn advance_to(&mut self, next: T) {
        assert!(next > self.now_at.inner);
        self.close_epoch();
        self.now_at = RootTimestamp::new(next);
        self.frontier.borrow_mut().update_weight(&self.now_at,  1, &mut (*self.progress.borrow_mut()));
    }

    pub fn close(self) { }

    pub fn epoch(&mut self) -> &T {
        &self.now_at.inner
    }
}

impl<T:Timestamp+Ord, D: Data> Drop for InputHelper<T, D> {
    fn drop(&mut self) {
        self.close_epoch();
    }
}
