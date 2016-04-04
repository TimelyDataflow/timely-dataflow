//! Create new `Streams` connected to external inputs.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Operate, Timestamp};
use progress::nested::subgraph::Source;
use progress::count_map::CountMap;
use progress::timestamp::RootTimestamp;
use progress::nested::product::Product;

use timely_communication::Allocate;
use {Data, Push};
use dataflow::channels::Content;
use dataflow::channels::pushers::{Tee, Counter as PushCounter};
use dataflow::channels::pushers::buffer::{Buffer as PushBuffer, Session};

use dataflow::operators::Capability;
use dataflow::operators::capability::mint as mint_capability;

use dataflow::{Stream, Scope};
use dataflow::scopes::{Child, Root};

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait UnorderedInput<G: Scope> {
    /// Create a new `Stream` and `Handle` through which to supply input.
    ///
    /// The `new_input` method returns a pair `(Handle, Stream)` where the `Stream` can be used
    /// immediately for timely dataflow construction, and the `Handle` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `Handle` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    /// ```
    fn new_unordered_input<D:Data>(&mut self) -> ((UnorderedHandle<G, D>, Capability<G::Timestamp>), Stream<G, D>);
}


impl<G: Scope> UnorderedInput<G> for G {
    fn new_unordered_input<D:Data>(&mut self) -> ((UnorderedHandle<G, D>, Capability<G::Timestamp>), Stream<G, D>) {

        let (output, registrar) = Tee::<G::Timestamp, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let (cap, helper) = UnorderedHandle::new(PushCounter::new(output, produced.clone()));
        let copies = self.peers();

        let index = self.add_operator(UnorderedOperator {
            progress: helper.progress.clone(),
            messages: produced.clone(),
            copies:   copies,
        });

        return ((helper, cap), Stream::new(Source { index: index, port: 0 }, registrar, self.clone()));
    }
}

struct UnorderedOperator<T:Timestamp> {
    progress:   Rc<RefCell<CountMap<T>>>,           // times closed since last asked
    messages:   Rc<RefCell<CountMap<T>>>,           // messages sent since last asked
    copies:     usize,
}

impl<T:Timestamp> Operate<T> for UnorderedOperator<T> {
    fn name(&self) -> String { "Input".to_owned() }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<T as Timestamp>::Summary>>>,
                                           Vec<CountMap<T>>) {
        let mut map = CountMap::new();
        map.update(&Default::default(), self.copies as i64 - 1);
        (Vec::new(), vec![map])
    }

    fn pull_internal_progress(&mut self,_messages_consumed: &mut [CountMap<T>],
                                         frontier_progress: &mut [CountMap<T>],
                                         messages_produced: &mut [CountMap<T>]) -> bool
    {
        self.messages.borrow_mut().drain_into(&mut messages_produced[0]);
        self.progress.borrow_mut().drain_into(&mut frontier_progress[0]);
        return false;
    }

    fn notify_me(&self) -> bool { false }
}


/// A handle to an input `Stream`, used to introduce data to a timely dataflow computation.
pub struct UnorderedHandle<G: Scope, D: Data> {
    progress: Rc<RefCell<CountMap<G::Timestamp>>>,           // times closed since last asked
    buffer: PushBuffer<G::Timestamp, D, PushCounter<G::Timestamp, D, Tee<G::Timestamp, D>>>,
}

impl<G: Scope, D: Data> UnorderedHandle<G, D> {
    fn new(pusher: PushCounter<G::Timestamp, D, Tee<G::Timestamp, D>>) -> (Capability<G::Timestamp>, UnorderedHandle<G, D>) {
        let progress = Rc::new(RefCell::new(CountMap::new()));
        let cap = mint_capability(Default::default(), progress.clone());
        let handle = UnorderedHandle {
            progress: progress,
            buffer: PushBuffer::new(pusher),
        };
        (cap, handle)
    }

    pub fn session<'b>(&'b mut self, cap: &Capability<G::Timestamp>) -> Session<'b, G::Timestamp, D, PushCounter<G::Timestamp, D, Tee<G::Timestamp, D>>> {
        self.buffer.session(cap)
    }

    pub fn flush(&mut self) {
        self.buffer.cease();
    }
}

impl<G: Scope, D: Data> Drop for UnorderedHandle<G, D> {
    fn drop(&mut self) {
        // TODO: explode if not all capabilities were given up?
    }
}
