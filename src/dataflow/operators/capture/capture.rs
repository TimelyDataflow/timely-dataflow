//! Traits and types for capturing timely dataflow streams.
//!
//! All timely dataflow streams can be captured, but there are many ways to capture
//! these streams. A stream may be `capture_into`'d any type implementing `EventPusher`,
//! and there are several default implementations, including a linked-list, Rust's MPSC
//! queue, and a binary serializer wrapping any `W: Write`.

use std::ops::DerefMut;

use ::Data;
use dataflow::{Scope, Stream};
use dataflow::channels::pact::{ParallelizationContract, Pipeline};
use dataflow::channels::pullers::Counter as PullCounter;

use progress::ChangeBatch;
use progress::nested::subgraph::Target;
use progress::{Timestamp, Operate, Antichain};

use super::{Event, EventPusher};

/// Capture a stream of timestamped data for later replay.
pub trait Capture<T: Timestamp, D: Data> {
    /// Captures a stream of timestamped data for later replay.
    ///
    /// #Examples
    ///
    /// The type `Rc<EventLink<T,D>>` implements a typed linked list,
    /// and can be captured into and replayed from.
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventLink, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(timely::Configuration::Thread, move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // these are to capture/replay the stream.
    ///     let handle1 = Rc::new(EventLink::new());
    ///     let handle2 = Some(handle1.clone());
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10).to_stream(scope1)
    ///                .capture_into(handle1)
    ///     );
    ///
    ///     worker.dataflow(|scope2| {
    ///         handle2.replay_into(scope2)
    ///                .capture_into(send)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    ///
    /// The types `EventWriter<T, D, W>` and `EventReader<T, D, R>` can be
    /// captured into and replayed from, respectively. They use binary writers
    /// and readers respectively, and can be backed by files, network sockets,
    /// etc.
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::net::{TcpListener, TcpStream};
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send0, recv0) = ::std::sync::mpsc::channel();
    /// let send0 = Arc::new(Mutex::new(send0));
    ///
    /// timely::execute(timely::Configuration::Thread, move |worker| {
    /// 
    ///     // this is only to validate the output.
    ///     let send0 = send0.lock().unwrap().clone();
    /// 
    ///     // these allow us to capture / replay a timely stream.
    ///     let list = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let send = TcpStream::connect("127.0.0.1:8000").unwrap();
    ///     let recv = list.incoming().next().unwrap().unwrap();
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10u64)
    ///             .to_stream(scope1)
    ///             .capture_into(EventWriter::new(send))
    ///     );
    ///
    ///     worker.dataflow::<u64,_,_>(|scope2| {
    ///         Some(EventReader::<_,u64,_>::new(recv))
    ///             .replay_into(scope2)
    ///             .capture_into(send0)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv0.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    fn capture_into<P: EventPusher<T, D>+'static>(&self, pusher: P);

    /// Captures a stream using Rust's MPSC channels.
    fn capture(&self) -> ::std::sync::mpsc::Receiver<Event<T, D>> {
        let (send, recv) = ::std::sync::mpsc::channel();
        self.capture_into(send);
        recv
    }
}

impl<S: Scope, D: Data> Capture<S::Timestamp, D> for Stream<S, D> {
    fn capture_into<P: EventPusher<S::Timestamp, D>+'static>(&self, pusher: P) {

        let mut scope = self.scope();   // clones the scope
        let channel_id = scope.new_identifier();

        let (sender, receiver) = Pipeline.connect(&mut scope, channel_id);
        let operator = CaptureOperator::new(PullCounter::new(receiver), pusher);

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);
    }
}

struct CaptureOperator<T: Timestamp, D: Data, P: EventPusher<T, D>> {
    input: PullCounter<T, D, <Pipeline as ParallelizationContract<T, D>>::Puller>,
    events: P,
}

impl<T:Timestamp, D: Data, P: EventPusher<T, D>> CaptureOperator<T, D, P> {
    fn new(input: PullCounter<T, D, <Pipeline as ParallelizationContract<T, D>>::Puller>, events: P) -> CaptureOperator<T, D, P> {
        CaptureOperator {
            input: input,
            events: events,
        }
    }
}

impl<T:Timestamp, D: Data, P: EventPusher<T, D>> Operate<T> for CaptureOperator<T, D, P> {

    fn name(&self) -> String { "Capture".to_owned() }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { 0 }

    // we need to set the initial value of the frontier
    fn set_external_summary(&mut self, _: Vec<Vec<Antichain<T::Summary>>>, counts: &mut [ChangeBatch<T>]) {

        // Any replayer will start with the assumption that we have `(Default::default(), 1)` as our initial
        // capability. Our first update should be to substitute the contents of `counts[0]` for that, which
        // may result in no change (if this is the initially expressed capability) but should still be correct.

        let mut map = counts[0].clone();
        map.update(Default::default(), -1);
        if !map.is_empty() {
            self.events.push(Event::Progress(map.into_inner()));
        }
        counts[0].clear();
    }

    // each change to the frontier should be shared
    fn push_external_progress(&mut self, counts: &mut [ChangeBatch<T>]) {
        // turn all received progress information into an event.
        self.events.push(Event::Progress(counts[0].clone().into_inner()));
        counts[0].clear();
    }

    fn pull_internal_progress(
        &mut self, 
        consumed: &mut [ChangeBatch<T>],  
        _: &mut [ChangeBatch<T>], 
        _: &mut [ChangeBatch<T>]) -> bool 
    {
        // turn each received message into an event.
        while let Some((time, data)) = self.input.next() {
            self.events.push(Event::Messages(time.clone(), data.deref_mut().clone()));
        }
        self.input.consumed().borrow_mut().drain_into(&mut consumed[0]);
        false
    }
}
