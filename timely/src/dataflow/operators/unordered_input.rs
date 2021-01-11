//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::{Schedule, ActivateOnDrop};

use crate::progress::frontier::Antichain;
use crate::progress::{Operate, operate::SharedProgress, Timestamp};
use crate::progress::Source;
use crate::progress::ChangeBatch;

use crate::Data;
use crate::dataflow::channels::pushers::{Tee, Counter as PushCounter};
use crate::dataflow::channels::pushers::buffer::{Buffer as PushBuffer, AutoflushSession};

use crate::dataflow::operators::ActivateCapability;
use crate::dataflow::operators::capability::mint as mint_capability;

use crate::dataflow::{Stream, Scope};

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait UnorderedInput<G: Scope> {
    /// Create a new capability-based `Stream` and `Handle` through which to supply input. This
    /// input supports multiple open epochs (timestamps) at the same time.
    ///
    /// The `new_unordered_input` method returns `((Handle, Capability), Stream)` where the `Stream` can be used
    /// immediately for timely dataflow construction, `Handle` and `Capability` are later used to introduce
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
    /// use timely::dataflow::operators::*;
    /// use timely::dataflow::operators::capture::Extract;
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
    ///         stream.capture_into(send);
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
    fn new_unordered_input<D:Data>(&mut self) -> ((UnorderedHandle<G::Timestamp, D>, ActivateCapability<G::Timestamp>), Stream<G, D>);
}


impl<G: Scope> UnorderedInput<G> for G {
    fn new_unordered_input<D:Data>(&mut self) -> ((UnorderedHandle<G::Timestamp, D>, ActivateCapability<G::Timestamp>), Stream<G, D>) {

        let (output, registrar) = Tee::<G::Timestamp, D>::new();
        let internal = Rc::new(RefCell::new(ChangeBatch::new()));
        // let produced = Rc::new(RefCell::new(ChangeBatch::new()));
        let cap = mint_capability(G::Timestamp::minimum(), internal.clone());
        let counter = PushCounter::new(output);
        let produced = counter.produced().clone();
        let peers = self.peers();

        let index = self.allocate_operator_index();
        let mut address = self.addr();
        address.push(index);

        let cap = ActivateCapability::new(cap, &address[..], self.activations().clone());

        let helper = UnorderedHandle::new(counter);

        self.add_operator_with_index(Box::new(UnorderedOperator {
            name: "UnorderedInput".to_owned(),
            address,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(0, 1))),
            internal,
            produced,
            peers,
        }), index);

        ((helper, cap), Stream::new(Source::new(index, 0), registrar, self.clone()))
    }
}

struct UnorderedOperator<T:Timestamp> {
    name: String,
    address: Vec<usize>,
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

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<T as Timestamp>::Summary>>>, Rc<RefCell<SharedProgress<T>>>) {
        let mut borrow = self.internal.borrow_mut();
        for (time, count) in borrow.drain() {
            self.shared_progress.borrow_mut().internals[0].update(time, count * (self.peers as i64));
        }
        (Vec::new(), self.shared_progress.clone())
    }

    fn notify_me(&self) -> bool { false }
}

/// A handle to an input `Stream`, used to introduce data to a timely dataflow computation.
pub struct UnorderedHandle<T: Timestamp, D: Data> {
    buffer: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
}

impl<T: Timestamp, D: Data> UnorderedHandle<T, D> {
    fn new(pusher: PushCounter<T, D, Tee<T, D>>) -> UnorderedHandle<T, D> {
        UnorderedHandle {
            buffer: PushBuffer::new(pusher),
        }
    }

    /// Allocates a new automatically flushing session based on the supplied capability.
    pub fn session<'b>(&'b mut self, cap: ActivateCapability<T>) -> ActivateOnDrop<AutoflushSession<'b, T, D, PushCounter<T, D, Tee<T, D>>>> {
        ActivateOnDrop::new(self.buffer.autoflush_session(cap.capability.clone()), cap.address.clone(), cap.activations.clone())
    }
}
