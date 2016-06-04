//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::Antichain;
use progress::{Operate, Timestamp};
use progress::nested::subgraph::Source;
use progress::count_map::CountMap;

use timely_communication::Allocate;
use Data;
use dataflow::channels::pushers::{Tee, Counter as PushCounter};
use dataflow::channels::pushers::buffer::{Buffer as PushBuffer, AutoflushSession};

use dataflow::operators::Capability;
use dataflow::operators::capability::mint as mint_capability;

use dataflow::{Stream, Scope};

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
    /// #Examples
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use timely::*;
    /// use timely::dataflow::operators::*;
    /// use timely::dataflow::operators::capture::Extract;
    /// use timely::dataflow::{Stream, Scope};
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(Configuration::Thread, move |root| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // create and capture the unordered input.
    ///     let (mut input, mut cap) = root.scoped(|scope| {
    ///         let (input, stream) = scope.new_unordered_input();
    ///         stream.capture_into(send);
    ///         input
    ///     });
    ///
    ///     // feed values 0..10 at times 0..10.
    ///     for round in 0..10 {
    ///         input.session(cap.clone()).give(round);
    ///         cap = cap.delayed(&RootTimestamp::new(round + 1));
    ///         root.step();
    ///     }
    /// }).unwrap();
    /// 
    /// let extract = recv.extract();
    /// for i in 0..10 {
    ///     assert_eq!(extract[i], (RootTimestamp::new(i), vec![i]));
    /// }
    /// ```
    fn new_unordered_input<D:Data>(&mut self) -> ((UnorderedHandle<G::Timestamp, D>, Capability<G::Timestamp>), Stream<G, D>);
}


impl<G: Scope> UnorderedInput<G> for G {
    fn new_unordered_input<D:Data>(&mut self) -> ((UnorderedHandle<G::Timestamp, D>, Capability<G::Timestamp>), Stream<G, D>) {

        let (output, registrar) = Tee::<G::Timestamp, D>::new();
        let internal = Rc::new(RefCell::new(CountMap::new()));
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let cap = mint_capability(Default::default(), internal.clone());
        let helper = UnorderedHandle::new(PushCounter::new(output, produced.clone()));
        let peers = self.peers();

        let index = self.add_operator(UnorderedOperator {
            internal: internal.clone(),
            produced: produced.clone(),
            peers:    peers,
        });

        return ((helper, cap), Stream::new(Source { index: index, port: 0 }, registrar, self.clone()));
    }
}

struct UnorderedOperator<T:Timestamp> {
    internal:   Rc<RefCell<CountMap<T>>>,
    produced:   Rc<RefCell<CountMap<T>>>,
    peers:     usize,
}

impl<T:Timestamp> Operate<T> for UnorderedOperator<T> {
    fn name(&self) -> String { "Input".to_owned() }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<T as Timestamp>::Summary>>>,
                                           Vec<CountMap<T>>) {
        let mut internal = CountMap::new();
        // augment the counts for each reserved capability.
        for &(ref time, count) in self.internal.borrow().iter() {
            internal.update(time, count * (self.peers as i64 - 1));
        }

        // drain the changes to empty out, and complete the counts for internal.
        self.internal.borrow_mut().drain_into(&mut internal);
        (Vec::new(), vec![internal])
    }

    fn pull_internal_progress(&mut self,_consumed: &mut [CountMap<T>],
                                         internal: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool
    {
        self.produced.borrow_mut().drain_into(&mut produced[0]);
        self.internal.borrow_mut().drain_into(&mut internal[0]);
        return false;
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
    pub fn session<'b>(&'b mut self, cap: Capability<T>) -> AutoflushSession<'b, T, D, PushCounter<T, D, Tee<T, D>>> {
        self.buffer.autoflush_session(cap)
    }
}

impl<T: Timestamp, D: Data> Drop for UnorderedHandle<T, D> {
    fn drop(&mut self) {
        // TODO: explode if not all capabilities were given up?
    }
}
