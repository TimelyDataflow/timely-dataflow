//! Create new `StreamCore`s connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;
use crate::{Container, Data};
use crate::container::{ContainerBuilder, CapacityContainerBuilder};

use crate::scheduling::{Schedule, ActivateOnDrop};

use crate::progress::{Operate, operate::SharedProgress, Timestamp};
use crate::progress::Source;
use crate::progress::ChangeBatch;
use crate::progress::operate::Connectivity;
use crate::dataflow::channels::pushers::{Counter, Tee};
use crate::dataflow::channels::pushers::buffer::{Buffer as PushBuffer, AutoflushSession};

use crate::dataflow::operators::{ActivateCapability, Capability};

use crate::dataflow::{Scope, StreamCore};

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait UnorderedInput<G: Scope> {
    /// Create a new capability-based [StreamCore] and [UnorderedHandle] through which to supply input. This
    /// input supports multiple open epochs (timestamps) at the same time.
    ///
    /// The `new_unordered_input_core` method returns `((HandleCore, Capability), StreamCore)` where the `StreamCore` can be used
    /// immediately for timely dataflow construction, `HandleCore` and `Capability` are later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `Capability` returned is for the default value of the timestamp type in use. The
    /// capability can be dropped to inform the system that the input has advanced beyond the
    /// capability's timestamp. To retain the ability to send, a new capability at a later timestamp
    /// should be obtained first, via the `delayed` function for `Capability`.
    ///
    /// To communicate the end-of-input drop all available capabilities.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use timely::*;
    /// use timely::dataflow::operators::{capture::Extract, Capture};
    /// use timely::dataflow::operators::core::{UnorderedInput};
    /// use timely::dataflow::Stream;
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // create and capture the unordered input.
    ///     let (mut input, mut cap) = worker.dataflow::<usize,_,_>(|scope| {
    ///         let (input, stream) = scope.new_unordered_input();
    ///         stream
    ///             .container::<Vec<_>>()
    ///             .capture_into(send);
    ///         input
    ///     });
    ///
    ///     // feed values 0..10 at times 0..10.
    ///     for round in 0..10 {
    ///         input.session(cap.clone()).give(round);
    ///         cap = cap.delayed(&(round + 1));
    ///         worker.step();
    ///     }
    /// }).unwrap();
    ///
    /// let extract = recv.extract();
    /// for i in 0..10 {
    ///     assert_eq!(extract[i], (i, vec![i]));
    /// }
    /// ```
    fn new_unordered_input<CB: ContainerBuilder>(&mut self) -> ((UnorderedHandle<G::Timestamp, CB>, ActivateCapability<G::Timestamp>), StreamCore<G, CB::Container>);
}

impl<G: Scope> UnorderedInput<G> for G {
    fn new_unordered_input<CB: ContainerBuilder>(&mut self) -> ((UnorderedHandle<G::Timestamp, CB>, ActivateCapability<G::Timestamp>), StreamCore<G, CB::Container>) {

        let (output, registrar) = Tee::<G::Timestamp, CB::Container>::new();
        let internal = Rc::new(RefCell::new(ChangeBatch::new()));
        // let produced = Rc::new(RefCell::new(ChangeBatch::new()));
        let cap = Capability::new(G::Timestamp::minimum(), Rc::clone(&internal));
        let counter = Counter::new(output);
        let produced = Rc::clone(counter.produced());
        let peers = self.peers();

        let index = self.allocate_operator_index();
        let address = self.addr_for_child(index);

        let cap = ActivateCapability::new(cap, Rc::clone(&address), self.activations());

        let helper = UnorderedHandle::new(counter);

        self.add_operator_with_index(Box::new(UnorderedOperator {
            name: "UnorderedInput".to_owned(),
            address,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(0, 1))),
            internal,
            produced,
            peers,
        }), index);

        ((helper, cap), StreamCore::new(Source::new(index, 0), registrar, self.clone()))
    }
}

struct UnorderedOperator<T:Timestamp> {
    name: String,
    address: Rc<[usize]>,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    internal:   Rc<RefCell<ChangeBatch<T>>>,
    produced:   Rc<RefCell<ChangeBatch<T>>>,
    peers:     usize,
}

impl<T:Timestamp> Schedule for UnorderedOperator<T> {
    fn name(&self) -> &str { &self.name }
    fn path(&self) -> &[usize] { &self.address[..] }
    fn schedule(&mut self) -> bool {
        let shared_progress = &mut *self.shared_progress.borrow_mut();
        self.internal.borrow_mut().drain_into(&mut shared_progress.internals[0]);
        self.produced.borrow_mut().drain_into(&mut shared_progress.produceds[0]);
        false
    }
}

impl<T:Timestamp> Operate<T> for UnorderedOperator<T> {
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Connectivity<<T as Timestamp>::Summary>, Rc<RefCell<SharedProgress<T>>>) {
        let mut borrow = self.internal.borrow_mut();
        for (time, count) in borrow.drain() {
            self.shared_progress.borrow_mut().internals[0].update(time, count * (self.peers as i64));
        }
        (Vec::new(), Rc::clone(&self.shared_progress))
    }

    fn notify_me(&self) -> bool { false }
}

/// A handle to an input [StreamCore], used to introduce data to a timely dataflow computation.
#[derive(Debug)]
pub struct UnorderedHandle<T: Timestamp, CB: ContainerBuilder> {
    buffer: PushBuffer<T, CB, Counter<T, CB::Container, Tee<T, CB::Container>>>,
}

impl<T: Timestamp, CB: ContainerBuilder> UnorderedHandle<T, CB> {
    fn new(pusher: Counter<T, CB::Container, Tee<T, CB::Container>>) -> UnorderedHandle<T, CB> {
        UnorderedHandle {
            buffer: PushBuffer::new(pusher),
        }
    }

    /// Allocates a new automatically flushing session based on the supplied capability.
    #[inline]
    pub fn session_with_builder(&mut self, cap: ActivateCapability<T>) -> ActivateOnDrop<AutoflushSession<'_, T, CB, Counter<T, CB::Container, Tee<T, CB::Container>>>> {
        ActivateOnDrop::new(self.buffer.autoflush_session_with_builder(cap.capability.clone()), Rc::clone(&cap.address), Rc::clone(&cap.activations))
    }
}

impl<T: Timestamp, C: Container + Data> UnorderedHandle<T, CapacityContainerBuilder<C>> {
    /// Allocates a new automatically flushing session based on the supplied capability.
    #[inline]
    pub fn session(&mut self, cap: ActivateCapability<T>) -> ActivateOnDrop<AutoflushSession<'_, T, CapacityContainerBuilder<C>, Counter<T, C, Tee<T, C>>>> {
        self.session_with_builder(cap)
    }
}
