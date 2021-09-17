//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::{Schedule, Activator};

use crate::progress::frontier::Antichain;
use crate::progress::{Operate, operate::SharedProgress, Timestamp, ChangeBatch};
use crate::progress::Source;

use crate::{Container, ContainerBuilder};
use crate::communication::Push;
use crate::dataflow::{ScopeParent, Scope, CoreStream};
use crate::dataflow::channels::{Message, pushers::CounterCore};

// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait InputCore : Scope {
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
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{InputCore, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input_core();
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
    fn new_input_core<D: Container>(&mut self) -> (HandleCore<<Self as ScopeParent>::Timestamp, D>, CoreStream<Self, D>)
        where <D as Container>::Builder: ContainerBuilder<Container=D>;

    /// Create a new stream from a supplied interactive handle.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate
    /// if it as attached to more than one stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{InputCore, Inspect};
    /// use timely::dataflow::operators::input_core::HandleCore;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = HandleCore::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_core_from(&mut input)
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
    fn input_core_from<D: Container>(&mut self, handle: &mut HandleCore<<Self as ScopeParent>::Timestamp, D>) -> CoreStream<Self, D>
        where <D as Container>::Builder: ContainerBuilder<Container=D>;
}

use crate::order::TotalOrder;
use crate::dataflow::channels::pushers::TeeCore;

impl<G: Scope> InputCore for G where <G as ScopeParent>::Timestamp: TotalOrder {
    fn new_input_core<D: Container>(&mut self) -> (HandleCore<<G as ScopeParent>::Timestamp, D>, CoreStream<G, D>)
        where <D as Container>::Builder: ContainerBuilder<Container=D>
    {
        let mut handle = HandleCore::new();
        let stream = self.input_core_from(&mut handle);
        (handle, stream)
    }

    fn input_core_from<D: Container>(&mut self, handle: &mut HandleCore<<G as ScopeParent>::Timestamp, D>) -> CoreStream<G, D>
        where <D as Container>::Builder: ContainerBuilder<Container=D>
    {

        let (output, registrar) = TeeCore::<<G as ScopeParent>::Timestamp, D, D::Allocation>::new();
        let counter = CounterCore::new(output);
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

        CoreStream::new(Source::new(index, 0), registrar, self.clone())
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

/// A handle to an input `Stream`, used to introduce data to a timely dataflow computation.
#[derive(Debug)]
pub struct HandleCore<T: Timestamp, C: Container>
where <C as Container>::Builder: ContainerBuilder<Container=C>
{
    activate: Vec<Activator>,
    progress: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    pushers: Vec<CounterCore<T, C, C::Allocation, TeeCore<T, C, C::Allocation>>>,
    buffer1: Option<C::Builder>,
    buffer2: Option<C>,
    now_at: T,
}

impl<T:Timestamp, D: Container> HandleCore<T, D>
    where <D as Container>::Builder: ContainerBuilder<Container=D>
{

    /// Allocates a new input handle, from which one can create timely streams.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{InputCore, Inspect};
    /// use timely::dataflow::operators::input_core::HandleCore;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = HandleCore::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_core_from(&mut input)
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
        HandleCore {
            activate: Vec::new(),
            progress: Vec::new(),
            pushers: Vec::new(),
            buffer1: None,
            buffer2: None,
            now_at: T::minimum(),
        }
    }

    /// Creates an input stream from the handle in the supplied scope.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{InputCore, Inspect};
    /// use timely::dataflow::operators::input_core::HandleCore;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = HandleCore::new();
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
    pub fn to_stream<G: Scope>(&mut self, scope: &mut G) -> CoreStream<G, D>
    where
        T: TotalOrder,
        G: ScopeParent<Timestamp=T>,
    {
        scope.input_core_from(self)
    }

    fn register(
        &mut self,
        pusher: CounterCore<T, D, D::Allocation, TeeCore<T, D, D::Allocation>>,
        progress: Rc<RefCell<ChangeBatch<T>>>
    ) {
        // flush current contents, so new registrant does not see existing data.
        if !self.buffer1.as_ref().map_or(false, D::Builder::is_empty) { self.flush(); }

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
        if let Some(builder1) = self.buffer1.take() {
            let mut buffer1 = builder1.build();
            let mut allocation = None;
            for index in 0..self.pushers.len() {
                if index < self.pushers.len() - 1 {
                    let copy = if let Some(allocation) = allocation.take() {
                        buffer1.clone_into(allocation)
                    } else {
                        buffer1.clone()
                    };
                    Message::push_at(Some(copy), self.now_at.clone(), &mut self.pushers[index], &mut allocation);
                } else {
                    // TODO: retain allocation
                    Message::push_at(Some(buffer1), self.now_at.clone(), &mut self.pushers[index], &mut allocation);
                    debug_assert!(buffer1.is_empty());
                }
            }
            let builder = D::Builder::with_allocation(buffer1);
            self.buffer1 = Some(builder);
        }
    }

    // closes the current epoch, flushing if needed, shutting if needed, and updating the frontier.
    fn close_epoch(&mut self) {
        if !self.buffer1.as_ref().map_or(true, D::Builder::is_empty) { self.flush(); }
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

    #[inline]
    /// Sends one record into the corresponding timely dataflow `Stream`, at the current epoch.
    pub fn send(&mut self, data: D::Inner) {
        // assert!(self.buffer1.capacity() == Message::<T, D>::default_length());
        if self.buffer1.is_none() {
            self.buffer1 = Some(D::Builder::new());
        }
        let buffer1 = self.buffer1.as_mut().unwrap();
        buffer1.push(data);
        if buffer1.len() == buffer1.capacity() {
            self.flush();
        }
    }

    /// Sends a batch of records into the corresponding timely dataflow `Stream`, at the current epoch.
    ///
    /// This method flushes single elements previously sent with `send`, to keep the insertion order.
    // pub fn send_batch(&mut self, buffer: &mut D) {
    //
    //     if !buffer.is_empty() {
    //         // flush buffered elements to ensure local fifo.
    //         if !self.buffer1.as_ref().map_or(true, D::Builder::is_empty) { self.flush(); }
    //
    //         // push buffer (or clone of buffer) at each destination.
    //         for index in 0 .. self.pushers.len() {
    //             if index < self.pushers.len() - 1 {
    //                 let builder = D::Builder::with_optional_allocation(&mut self.buffer2);
    //                 builder.initialize_from(&buffer);
    //                 let mut buffer = Some(builder.build());
    //                 Message::push_at(&mut buffer, self.now_at.clone(), &mut self.pushers[index]);
    //                 self.buffer2 = buffer;
    //             }
    //             else {
    //                 // TODO: retain allocation
    //                 Message::push_at(&mut Some(buffer), self.now_at.clone(), &mut self.pushers[index]);
    //                 assert!(buffer.is_empty());
    //             }
    //         }
    //         // TODO: Cannot clear `buffer` because containers are immutable
    //         // buffer.clear();
    //     }
    // }

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

impl<T: Timestamp, D: Container> Default for HandleCore<T, D>
    where <D as Container>::Builder: ContainerBuilder<Container=D>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T:Timestamp, D: Container> Drop for HandleCore<T, D>
    where <D as Container>::Builder: ContainerBuilder<Container=D>
{
    fn drop(&mut self) {
        self.close_epoch();
    }
}
