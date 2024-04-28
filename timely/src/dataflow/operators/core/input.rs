//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::container::{PushContainer, PushInto};

use crate::scheduling::{Schedule, Activator};

use crate::progress::frontier::Antichain;
use crate::progress::{Operate, operate::SharedProgress, Timestamp, ChangeBatch};
use crate::progress::Source;

use crate::Container;
use crate::communication::Push;
use crate::dataflow::{Scope, ScopeParent, StreamCore};
use crate::dataflow::channels::pushers::{Tee, Counter};
use crate::dataflow::channels::Message;


// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait Input : Scope {
    /// Create a new [StreamCore] and [Handle] through which to supply input.
    ///
    /// The `new_input` method returns a pair `(Handle, StreamCore)` where the [StreamCore] can be used
    /// immediately for timely dataflow construction, and the `Handle` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `Handle` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::core::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input::<Vec<_>>();
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
    fn new_input<C: Container>(&mut self) -> (Handle<<Self as ScopeParent>::Timestamp, C>, StreamCore<Self, C>);

    /// Create a new stream from a supplied interactive handle.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate
    /// if it as attached to more than one stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::core::{Input, Inspect};
    /// use timely::dataflow::operators::core::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .container::<Vec<_>>()
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
    fn input_from<C: Container>(&mut self, handle: &mut Handle<<Self as ScopeParent>::Timestamp, C>) -> StreamCore<Self, C>;
}

use crate::order::TotalOrder;
impl<G: Scope> Input for G where <G as ScopeParent>::Timestamp: TotalOrder {
    fn new_input<C: Container>(&mut self) -> (Handle<<G as ScopeParent>::Timestamp, C>, StreamCore<G, C>) {
        let mut handle = Handle::new();
        let stream = self.input_from(&mut handle);
        (handle, stream)
    }

    fn input_from<C: Container>(&mut self, handle: &mut Handle<<G as ScopeParent>::Timestamp, C>) -> StreamCore<G, C> {
        let (output, registrar) = Tee::<<G as ScopeParent>::Timestamp, C>::new();
        let counter = Counter::new(output);
        let produced = counter.produced().clone();

        let index = self.allocate_operator_index();
        let mut address = self.addr();
        address.push(index);

        handle.activate.push(self.activator_for(&address[..]));

        let progress = Rc::new(RefCell::new(ChangeBatch::new()));

        handle.register(counter, progress.clone());

        let copies = self.peers();

        self.add_operator_with_index(Box::new(Operator {
            name: "Input".to_owned(),
            address,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(0, 1))),
            progress,
            messages: produced,
            copies,
        }), index);

        StreamCore::new(Source::new(index, 0), registrar, self.clone())
    }
}

#[derive(Debug)]
struct Operator<T:Timestamp> {
    name: String,
    address: Vec<usize>,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    progress:   Rc<RefCell<ChangeBatch<T>>>,           // times closed since last asked
    messages:   Rc<RefCell<ChangeBatch<T>>>,           // messages sent since last asked
    copies:     usize,
}

impl<T:Timestamp> Schedule for Operator<T> {

    fn name(&self) -> &str { &self.name }

    fn path(&self) -> &[usize] { &self.address[..] }

    fn schedule(&mut self) -> bool {
        let shared_progress = &mut *self.shared_progress.borrow_mut();
        self.progress.borrow_mut().drain_into(&mut shared_progress.internals[0]);
        self.messages.borrow_mut().drain_into(&mut shared_progress.produceds[0]);
        false
    }
}

impl<T:Timestamp> Operate<T> for Operator<T> {

    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<T as Timestamp>::Summary>>>, Rc<RefCell<SharedProgress<T>>>) {
        self.shared_progress.borrow_mut().internals[0].update(T::minimum(), self.copies as i64);
        (Vec::new(), self.shared_progress.clone())
    }

    fn notify_me(&self) -> bool { false }
}


/// A handle to an input `StreamCore`, used to introduce data to a timely dataflow computation.
#[derive(Debug)]
pub struct Handle<T: Timestamp, C: Container> {
    activate: Vec<Activator>,
    progress: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    pushers: Vec<Counter<T, C, Tee<T, C>>>,
    buffer1: C,
    buffer2: C,
    now_at: T,
}

impl<T: Timestamp, C: Container> Handle<T, C> {
    /// Allocates a new input handle, from which one can create timely streams.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::core::{Input, Inspect};
    /// use timely::dataflow::operators::core::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .container::<Vec<_>>()
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
        Self {
            activate: Vec::new(),
            progress: Vec::new(),
            pushers: Vec::new(),
            buffer1: Default::default(),
            buffer2: Default::default(),
            now_at: T::minimum(),
        }
    }

    /// Creates an input stream from the handle in the supplied scope.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::core::{Input, Inspect};
    /// use timely::dataflow::operators::core::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         input.to_stream(scope)
    ///              .container::<Vec<_>>()
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
    pub fn to_stream<G: Scope>(&mut self, scope: &mut G) -> StreamCore<G, C>
    where
        T: TotalOrder,
        G: ScopeParent<Timestamp=T>,
    {
        scope.input_from(self)
    }

    fn register(
        &mut self,
        pusher: Counter<T, C, Tee<T, C>>,
        progress: Rc<RefCell<ChangeBatch<T>>>,
    ) {
        // flush current contents, so new registrant does not see existing data.
        if !self.buffer1.is_empty() { self.flush(); }

        // we need to produce an appropriate update to the capabilities for `progress`, in case a
        // user has decided to drive the handle around a bit before registering it.
        progress.borrow_mut().update(T::minimum(), -1);
        progress.borrow_mut().update(self.now_at.clone(), 1);

        self.progress.push(progress);
        self.pushers.push(pusher);
    }

    // flushes our buffer at each of the destinations. there can be more than one; clone if needed.
    #[inline(never)]
    fn flush(&mut self) {
        for index in 0 .. self.pushers.len() {
            if index < self.pushers.len() - 1 {
                self.buffer2.clone_from(&self.buffer1);
                Message::push_at(&mut self.buffer2, self.now_at.clone(), &mut self.pushers[index]);
                debug_assert!(self.buffer2.is_empty());
            }
            else {
                Message::push_at(&mut self.buffer1, self.now_at.clone(), &mut self.pushers[index]);
                debug_assert!(self.buffer1.is_empty());
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
        // Alert worker of each active input operator.
        for activate in self.activate.iter() {
            activate.activate();
        }
    }

    /// Sends a batch of records into the corresponding timely dataflow [StreamCore], at the current epoch.
    ///
    /// This method flushes single elements previously sent with `send`, to keep the insertion order.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::core::{Input, InspectCore};
    /// use timely::dataflow::operators::core::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect_container(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send_batch(&mut vec![format!("{}", round)]);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn send_batch(&mut self, buffer: &mut C) {

        if !buffer.is_empty() {
            // flush buffered elements to ensure local fifo.
            if !self.buffer1.is_empty() { self.flush(); }

            // push buffer (or clone of buffer) at each destination.
            for index in 0 .. self.pushers.len() {
                if index < self.pushers.len() - 1 {
                    self.buffer2.clone_from(&buffer);
                    Message::push_at(&mut self.buffer2, self.now_at.clone(), &mut self.pushers[index]);
                    assert!(self.buffer2.is_empty());
                }
                else {
                    Message::push_at(buffer, self.now_at.clone(), &mut self.pushers[index]);
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
        // Assert that we do not rewind time.
        assert!(self.now_at.less_equal(&next));
        // Flush buffers if time has actually changed.
        if !self.now_at.eq(&next) {
            self.close_epoch();
            self.now_at = next;
            for progress in self.progress.iter() {
                progress.borrow_mut().update(self.now_at.clone(), 1);
            }
        }
    }

    /// Closes the input.
    ///
    /// This method allows timely dataflow to issue all progress notifications blocked by this input
    /// and to begin to shut down operators, as this input can no longer produce data.
    pub fn close(self) { }

    /// Reports the current epoch.
    pub fn epoch(&self) -> &T {
        &self.now_at
    }

    /// Reports the current timestamp.
    pub fn time(&self) -> &T {
        &self.now_at
    }
}

impl<T: Timestamp, C: PushContainer> Handle<T, C> {
    #[inline]
    /// Sends one record into the corresponding timely dataflow `Stream`, at the current epoch.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::core::{Input, Inspect};
    /// use timely::dataflow::operators::core::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .container::<Vec<_>>()
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
    pub fn send<D: PushInto<C>>(&mut self, data: D) {
        self.buffer1.push(data);
        if self.buffer1.len() == self.buffer1.capacity() {
            self.flush();
        }
    }
}

impl<T: Timestamp, C: Container> Default for Handle<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T:Timestamp, C: Container> Drop for Handle<T, C> {
    fn drop(&mut self) {
        self.close_epoch();
    }
}
