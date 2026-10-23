//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::container::{CapacityContainerBuilder, PushInto};
use crate::scheduling::{Schedule, Activator};

use crate::progress::{Operate, operate::SharedProgress, Timestamp, ChangeBatch};
use crate::progress::Source;
use crate::progress::operate::Connectivity;
use crate::{Container, ContainerBuilder};
use crate::communication::Push;
use crate::dataflow::{Scope, Stream};
use crate::dataflow::channels::pushers::{Tee, Counter};
use crate::dataflow::channels::Message;


// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

/// Create streams with either container input or buffered element input.
pub trait Input<'scope> {
    /// The timestamp at which this input scope operates.
    type Timestamp: Timestamp;

    /// Create an input that accepts complete containers without a container builder.
    ///
    /// ```
    /// use timely::dataflow::operators::{Input, InspectCore};
    /// timely::execute_directly(|worker| {
    ///     let mut input = worker.dataflow::<u64, _, _>(|scope| {
    ///         let (input, stream) = scope.new_input::<Vec<u64>>();
    ///         stream.inspect_container(|data| println!("{data:?}"));
    ///         input
    ///     });
    ///     input.send_batch(&mut vec![1, 2, 3]);
    ///     input.advance_to(1);
    /// });
    /// ```
    fn new_input<C: Container+Clone>(&self) -> (Handle<Self::Timestamp, C>, Stream<'scope, Self::Timestamp, C>);

    /// Create a new [Stream] and [BufferedHandle] through which to supply input.
    ///
    /// The `new_input_with_builder` method returns a pair `(BufferedHandle, Stream)` where the [Stream] can be used
    /// immediately for timely dataflow construction, and the `BufferedHandle` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `BufferedHandle` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, InspectCore};
    /// use timely::container::CapacityContainerBuilder;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input_with_builder::<CapacityContainerBuilder<Vec<_>>>();
    ///         stream.inspect_container(|x| println!("hello {:?}", x));
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
    fn new_input_with_builder<CB: ContainerBuilder<Container: Clone>>(&self) -> (BufferedHandle<Self::Timestamp, CB>, Stream<'scope, Self::Timestamp, CB::Container>);

    /// Create a new stream from a supplied container input handle.
    ///
    /// Attach all streams before consuming the handle with [`Handle::with_builder`]
    /// or [`Handle::with_default_builder`] for element input.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate
    /// if it as attached to more than one stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::InputHandle;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = InputHandle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .container::<Vec<_>>()
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///     let mut input = input.with_default_builder();
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn input_from<C: Container+Clone>(&self, handle: &mut Handle<Self::Timestamp, C>) -> Stream<'scope, Self::Timestamp, C>;
}

use crate::order::TotalOrder;
impl<'scope, T: Timestamp + TotalOrder> Input<'scope> for Scope<'scope, T> {
    type Timestamp = T;
    fn new_input<C: Container+Clone>(&self) -> (Handle<T, C>, Stream<'scope, T, C>) {
        let mut handle = Handle::new();
        let stream = self.input_from(&mut handle);
        (handle, stream)
    }

    fn new_input_with_builder<CB: ContainerBuilder<Container: Clone>>(&self) -> (BufferedHandle<T, CB>, Stream<'scope, T, CB::Container>) {
        let (handle, stream) = self.new_input();
        (handle.with_builder(), stream)
    }

    fn input_from<C: Container+Clone>(&self, handle: &mut Handle<T, C>) -> Stream<'scope, T, C> {
        let (output, registrar) = Tee::<T, C>::new();
        let counter = Counter::new(output);
        let produced = Rc::clone(counter.produced());

        let slot = self.reserve_operator();
        let index = slot.index();
        let address = slot.addr();

        handle.activate.push(self.activator_for(Rc::clone(&address)));

        let progress = Rc::new(RefCell::new(ChangeBatch::new()));

        handle.register(counter, Rc::clone(&progress));

        let copies = self.peers();

        slot.install(Box::new(Operator {
            name: "Input".to_owned(),
            address,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(0, 1))),
            progress,
            messages: produced,
            copies,
        }));

        Stream::new(Source::new(index, 0), registrar, *self)
    }
}

#[derive(Debug)]
struct Operator<T:Timestamp> {
    name: String,
    address: Rc<[usize]>,
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

use crate::progress::operate::FrontierInterest;
impl<T:Timestamp> Operate<T> for Operator<T> {

    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn initialize(self: Box<Self>) -> (Connectivity<<T as Timestamp>::Summary>, Rc<RefCell<SharedProgress<T>>>, Box<dyn Schedule>) {
        self.shared_progress.borrow_mut().internals[0].update(T::minimum(), self.copies as i64);
        (Vec::new(), Rc::clone(&self.shared_progress), self)
    }

    fn notify_me(&self) -> &[FrontierInterest] { &[] }
}


/// An element input handle that buffers records using a container builder.
///
/// Use [`Handle`] to send complete containers instead. This handle exposes
/// only element input, so there is no ordering to reconcile between the two modes.
/// Record ordering within containers is determined by the builder.
/// Attach streams to a [`Handle`] before consuming it with [`Handle::with_builder`]
/// or [`Handle::with_default_builder`] to create this wrapper.
///
/// ```compile_fail
/// use timely::dataflow::InputHandle;
/// let mut input = InputHandle::<u64, Vec<u64>>::new().with_default_builder();
/// input.send_batch(&mut vec![1]);
/// ```
#[derive(Debug)]
pub struct BufferedHandle<T: Timestamp, CB: ContainerBuilder<Container: Clone>> {
    inner: Handle<T, CB::Container>,
    builder: CB,
}

impl<T: Timestamp, CB: ContainerBuilder<Container: Clone>> BufferedHandle<T, CB> {
    /// Flush all contents and distribute to downstream operators.
    #[inline]
    pub fn flush(&mut self) {
        while let Some(container) = self.builder.finish() {
            self.inner.send_batch(container);
        }
    }

    /// Sends one record into the corresponding timely dataflow `Stream`, at the current epoch.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::InputHandle;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = InputHandle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .container::<Vec<_>>()
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///     let mut input = input.with_default_builder();
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    #[inline]
    pub fn send<D>(&mut self, data: D) where CB: PushInto<D> {
        self.push_into(data)
    }

    /// Advances the current epoch to `next`, flushing buffered records when it changes.
    pub fn advance_to(&mut self, next: T) {
        assert!(self.time().less_equal(&next));
        if self.time() != &next {
            self.flush();
            self.inner.advance_to(next);
        }
    }

    /// Closes the input, flushing buffered records and releasing progress holds.
    pub fn close(self) { }

    /// Reports the current timestamp.
    pub fn time(&self) -> &T { self.inner.time() }
}

impl<T, CB, D> PushInto<D> for BufferedHandle<T, CB>
where
    T: Timestamp,
    CB: ContainerBuilder<Container: Clone> + PushInto<D>,
{
    #[inline]
    fn push_into(&mut self, item: D) {
        self.builder.push_into(item);
        while let Some(container) = self.builder.extract() {
            self.inner.send_batch(container);
        }
    }
}

impl<T: Timestamp, CB: ContainerBuilder<Container: Clone>> Drop for BufferedHandle<T, CB> {
    fn drop(&mut self) { self.flush(); }
}

/// A container input handle with no element buffer or container builder.
///
/// Use [`BufferedHandle`] to build containers from individual records instead.
///
/// ```compile_fail
/// use timely::dataflow::InputHandle;
/// let mut input = InputHandle::<u64, Vec<u64>>::new();
/// input.send(1);
/// ```
#[derive(Debug)]
pub struct Handle<T: Timestamp, C: Container+Clone> {
    activate: Vec<Activator>,
    progress: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    pushers: Vec<Counter<T, Tee<T, C>>>,
    buffer: C,
    now_at: T,
}

impl<T: Timestamp, C: Container+Clone> Handle<T, C> {
    /// Allocates a handle that accepts complete containers.
    pub fn new() -> Self {
        Self {
            activate: Vec::new(),
            progress: Vec::new(),
            pushers: Vec::new(),
            buffer: C::default(),
            now_at: T::minimum(),
        }
    }

    /// Consumes this handle and wraps it in the default builder for element input.
    ///
    /// The default is [`CapacityContainerBuilder<C>`], which batches records
    /// according to the container's preferred capacity and preserves FIFO order.
    /// Attached streams and the current timestamp are preserved.
    ///
    /// ```
    /// use timely::dataflow::InputHandle;
    /// let mut input = InputHandle::<u64, Vec<u64>>::new().with_default_builder();
    /// input.send(1);
    /// input.advance_to(1);
    /// ```
    pub fn with_default_builder(self) -> BufferedHandle<T, CapacityContainerBuilder<C>> {
        self.with_builder()
    }

    /// Consumes this handle and wraps it in a builder for element input.
    ///
    /// Attached streams and the current timestamp are preserved. The wrapper
    /// exposes element input only and flushes its builder on advance and drop.
    ///
    /// ```
    /// use timely::dataflow::InputHandle;
    /// use timely::container::CapacityContainerBuilder;
    /// let mut input = InputHandle::<u64, Vec<u64>>::new()
    ///     .with_builder::<CapacityContainerBuilder<_>>();
    /// input.send(1);
    /// input.advance_to(1);
    /// ```
    pub fn with_builder<CB: ContainerBuilder<Container = C>>(self) -> BufferedHandle<T, CB> {
        BufferedHandle { inner: self, builder: CB::default() }
    }

    /// Creates an input stream from this handle in the supplied scope.
    pub fn to_stream<'scope>(&mut self, scope: Scope<'scope, T>) -> Stream<'scope, T, C>
    where T: TotalOrder,
    {
        scope.input_from(self)
    }

    fn register(
        &mut self,
        pusher: Counter<T, Tee<T, C>>,
        progress: Rc<RefCell<ChangeBatch<T>>>,
    ) {
        // we need to produce an appropriate update to the capabilities for `progress`, in case a
        // user has decided to drive the handle around a bit before registering it.
        progress.borrow_mut().update(T::minimum(), -1);
        progress.borrow_mut().update(self.now_at.clone(), 1);

        self.progress.push(progress);
        self.pushers.push(pusher);
    }

    /// Closes the current epoch, completing sends and updating the frontier.
    // TODO: Find a better name for this function.
    fn close_epoch(&mut self) {
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

    /// Sends a batch of records into the corresponding timely dataflow [Stream], at the current epoch.
    ///
    /// The container is left in an undefined state after sending.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::InputHandle;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = InputHandle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect_core(|x| println!("hello {:?}", x));
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
    #[inline]
    pub fn send_batch(&mut self, container: &mut C) {
        if !container.is_empty() {
            for index in 0 .. self.pushers.len() {
                let buffer = if index < self.pushers.len() - 1 {
                    self.buffer.clone_from(container);
                    &mut self.buffer
                }
                else {
                    &mut *container
                };
                Message::push_at(buffer, crate::progress::Stamp::from_elem(self.now_at.clone()), &mut self.pushers[index]);
            }
        }
    }

    /// Advances the current epoch to `next`.
    ///
    /// This method allows timely dataflow to issue progress notifications as it can now determine
    /// that this input can no longer produce data at earlier timestamps.
    pub fn advance_to(&mut self, next: T) {
        // Assert that we do not rewind time.
        assert!(self.now_at.less_equal(&next));
        // Close the epoch if time has actually changed.
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

    /// Reports the current timestamp.
    pub fn time(&self) -> &T {
        &self.now_at
    }
}

impl<T: Timestamp, C: Container+Clone> Default for Handle<T, C> {
    fn default() -> Self { Self::new() }
}

impl<T: Timestamp, C: Container+Clone> Drop for Handle<T, C> {
    fn drop(&mut self) { self.close_epoch(); }
}
