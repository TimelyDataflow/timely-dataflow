//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::Antichain;
use progress::{Operate, Timestamp};
use progress::nested::subgraph::Source;
use progress::ChangeBatch;
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

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait Input<'a, A: Allocate, T: Timestamp> {
    /// Create a new `Stream` and `Handle` through which to supply input.
    ///
    /// The `new_input` method returns a pair `(Handle, Stream)` where the `Stream` can be used
    /// immediately for timely dataflow construction, and the `Handle` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `Handle` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    ///
    /// #Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Configuration::Thread, |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input();
    ///         stream.inspect(|x| println!("hello {:?}", x));
    ///         input
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn new_input<D: Data>(&mut self) -> (Handle<T, D>, Stream<Child<'a, Root<A>, T>, D>);

    /// Create a new stream from a supplied interactive handle.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate 
    /// if it as attached to more than one stream.
    ///
    /// #Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Configuration::Thread, |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn input_from<D: Data>(&mut self, handle: &mut Handle<T, D>) -> Stream<Child<'a, Root<A>, T>, D>;
}

impl<'a, A: Allocate, T: Timestamp> Input<'a, A, T> for Child<'a, Root<A>, T> {
    fn new_input<D: Data>(&mut self) -> (Handle<T, D>, Stream<Child<'a, Root<A>, T>, D>) {
        let mut handle = Handle::new();
        let stream = self.input_from(&mut handle);
        (handle, stream)
    }

    fn input_from<D: Data>(&mut self, handle: &mut Handle<T, D>) -> Stream<Child<'a, Root<A>, T>, D> {

        let (output, registrar) = Tee::<Product<RootTimestamp, T>, D>::new();
        let counter = Counter::new(output);
        let produced = counter.produced().clone();

        let progress = Rc::new(RefCell::new(ChangeBatch::new()));

        handle.register(counter, progress.clone());

        let copies = self.peers();

        let index = self.add_operator(Operator {
            progress: progress,
            messages: produced,
            copies:   copies,
        });

        Stream::new(Source { index: index, port: 0 }, registrar, self.clone())        
    }
}

struct Operator<T:Timestamp> {
    progress:   Rc<RefCell<ChangeBatch<Product<RootTimestamp, T>>>>,           // times closed since last asked
    messages:   Rc<RefCell<ChangeBatch<Product<RootTimestamp, T>>>>,           // messages sent since last asked
    copies:     usize,
}

impl<T:Timestamp> Operate<Product<RootTimestamp, T>> for Operator<T> {
    fn name(&self) -> String { "Input".to_owned() }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<Product<RootTimestamp, T> as Timestamp>::Summary>>>,
                                           Vec<ChangeBatch<Product<RootTimestamp, T>>>) {
        let mut map = ChangeBatch::new();
        map.update(Default::default(), self.copies as i64);
        (Vec::new(), vec![map])
    }

    fn pull_internal_progress(&mut self,_messages_consumed: &mut [ChangeBatch<Product<RootTimestamp, T>>],
                                         frontier_progress: &mut [ChangeBatch<Product<RootTimestamp, T>>],
                                         messages_produced: &mut [ChangeBatch<Product<RootTimestamp, T>>]) -> bool
    {
        self.messages.borrow_mut().drain_into(&mut messages_produced[0]);
        self.progress.borrow_mut().drain_into(&mut frontier_progress[0]);
        false
    }

    fn notify_me(&self) -> bool { false }
}


/// A handle to an input `Stream`, used to introduce data to a timely dataflow computation.
pub struct Handle<T: Timestamp, D: Data> {
    progress: Vec<Rc<RefCell<ChangeBatch<Product<RootTimestamp, T>>>>>,
    pushers: Vec<Counter<Product<RootTimestamp, T>, D, Tee<Product<RootTimestamp, T>, D>>>,
    buffer1: Vec<D>,
    buffer2: Vec<D>,
    now_at: Product<RootTimestamp, T>,
}

impl<T:Timestamp, D: Data> Handle<T, D> {

    /// Allocates a new input handle, from which one can create timely streams.
    ///
    /// #Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Configuration::Thread, |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn new() -> Self {
        Handle {
            progress: Vec::new(),
            pushers: Vec::new(),
            buffer1: Vec::with_capacity(Content::<D>::default_length()),
            buffer2: Vec::with_capacity(Content::<D>::default_length()),
            now_at: Default::default(),
        }
    }

    /// Creates an input stream from the handle in the supplied scope.
    ///
    /// #Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Configuration::Thread, |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         input.to_stream(scope)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn to_stream<'a, A: Allocate>(&mut self, scope: &mut Child<'a, Root<A>, T>) -> Stream<Child<'a, Root<A>, T>, D> 
    where T: Ord {
        scope.input_from(self)
    }

    fn register(
        &mut self,
        pusher: Counter<Product<RootTimestamp, T>, D, Tee<Product<RootTimestamp, T>, D>>,
        progress: Rc<RefCell<ChangeBatch<Product<RootTimestamp, T>>>>
    ) {
        // flush current contents, so new registrant does not see existing data.
        if !self.buffer1.is_empty() { self.flush(); }

        // we need to produce an appropriate update to the capabilities for `progress`, in case a
        // user has decided to drive the handle around a bit before registering it.
        progress.borrow_mut().update(Default::default(), -1);
        progress.borrow_mut().update(self.now_at.clone(), 1);

        self.progress.push(progress);
        self.pushers.push(pusher);
    }

    // flushes our buffer at each of the destinations. there can be more than one; clone if needed.
    fn flush(&mut self) {
        for index in 0 .. self.pushers.len() {
            if index < self.pushers.len() - 1 {
                self.buffer2.extend_from_slice(&self.buffer1[..]);
                Content::push_at(&mut self.buffer2, self.now_at.clone(), &mut self.pushers[index]);
                assert!(self.buffer2.is_empty());
            }
            else {
                Content::push_at(&mut self.buffer1, self.now_at.clone(), &mut self.pushers[index]);
                assert!(self.buffer1.is_empty());
            }
        }
        self.buffer1.clear();
    }

    // closes the current epoch, flushing if needed, shutting if needed, and updating the frontier.
    fn close_epoch(&mut self) {
        if !self.buffer1.is_empty() { self.flush(); }
        for pusher in self.pushers.iter_mut() {
            pusher.done();
        }
        for progress in self.progress.iter() {
            progress.borrow_mut().update(self.now_at.clone(), -1);
        }
    }

    #[inline(always)]
    /// Sends one record into the corresponding timely dataflow `Stream`, at the current epoch.
    pub fn send(&mut self, data: D) {
        // assert!(self.buffer.capacity() == Content::<D>::default_length());
        self.buffer1.push(data);
        if self.buffer1.len() == self.buffer1.capacity() {
            self.flush();
        }
    }

    /// Sends a batch of records into the corresponding timely dataflow `Stream`, at the current epoch.
    ///
    /// This method flushes single elements previously sent with `send`, to keep the insertion order.
    pub fn send_batch(&mut self, buffer: &mut Vec<D>) {

        if !buffer.is_empty() {
            // flush buffered elements to ensure local fifo.
            if !self.buffer1.is_empty() { self.flush(); }

            // push buffer (or clone of buffer) at each destination.
            for index in 0 .. self.pushers.len() {
                if index < self.pushers.len() - 1 {
                    self.buffer2.extend_from_slice(&buffer[..]);
                    Content::push_at(&mut self.buffer2, self.now_at.clone(), &mut self.pushers[index]);
                    assert!(self.buffer2.is_empty());
                }
                else {
                    Content::push_at(buffer, self.now_at.clone(), &mut self.pushers[index]);
                    assert!(buffer.is_empty());
                }
            }
            buffer.clear();
        }
    }

    /// Advances the current epoch to `next`.
    ///
    /// This method allows timely dataflow to issue progress notifications as it can now determine
    /// that this input can no longer produce data at earlier timestamps.
    pub fn advance_to(&mut self, next: T) {
        assert!(self.now_at.inner.less_equal(&next));
        self.close_epoch();
        self.now_at = RootTimestamp::new(next);
        for progress in self.progress.iter() {
            progress.borrow_mut().update(self.now_at.clone(), 1);
        }
    }

    /// Closes the input.
    ///
    /// This method allows timely dataflow to issue all progress notifications blocked by this input
    /// and to begin to shut down operators, as this input can no longer produce data.
    pub fn close(self) { }

    /// Reports the current epoch.
    pub fn epoch(&self) -> &T {
        &self.now_at.inner
    }

    /// Reports the current timestamp.
    pub fn time(&self) -> &Product<RootTimestamp, T> {
        &self.now_at
    }
}

impl<T:Timestamp, D: Data> Drop for Handle<T, D> {
    fn drop(&mut self) {
        self.close_epoch();
    }
}
