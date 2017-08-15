//! Operators which capture and replay streams of records.
//!
//! The `capture_into` and `replay_into` operators respectively capture what a unary operator
//! sees as input (both data and progress information), and play this information back as a new
//! input.
//!
//! The `capture_into` method requires a `P: EventPusher<T, D>`, which is some type accepting 
//! `Event<T, D>` inputs. This module provides several examples, including the linked list 
//! `EventLink<T, D>`, and the binary `EventWriter<T, D, W>` wrapping any `W: Write`.
//!
//! #Examples
//!
//! The type `Rc<EventLink<T,D>>` implements a typed linked list,
//! and can be captured into and replayed from.
//!
//! ```
//! use std::rc::Rc;
//! use timely::dataflow::Scope;
//! use timely::dataflow::operators::{Capture, ToStream, Inspect};
//! use timely::dataflow::operators::capture::{EventLink, Replay};
//!
//! timely::execute(timely::Configuration::Thread, |worker| {
//!     let handle1 = Rc::new(EventLink::new());
//!     let handle2 = handle1.clone();
//!
//!     worker.dataflow::<u64,_,_>(|scope1|
//!         (0..10).to_stream(scope1)
//!                .capture_into(handle1)
//!     );
//!
//!     worker.dataflow(|scope2| {
//!         handle2.replay_into(scope2)
//!                .inspect(|x| println!("replayed: {:?}", x));
//!     })
//! }).unwrap();
//! ```
//!
//! The types `EventWriter<T, D, W>` and `EventReader<T, D, R>` can be
//! captured into and replayed from, respectively. The use binary writers
//! and readers respectively, and can be backed by files, network sockets,
//! etc.
//!
//! ```
//! use std::rc::Rc;
//! use std::net::{TcpListener, TcpStream};
//! use timely::dataflow::Scope;
//! use timely::dataflow::operators::{Capture, ToStream, Inspect};
//! use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay};
//!
//! timely::execute(timely::Configuration::Thread, |worker| {
//!     let list = TcpListener::bind("127.0.0.1:8000").unwrap();
//!     let send = TcpStream::connect("127.0.0.1:8000").unwrap();
//!     let recv = list.incoming().next().unwrap().unwrap();
//!
//!     worker.dataflow::<u64,_,_>(|scope1|
//!         (0..10u64)
//!             .to_stream(scope1)
//!             .capture_into(EventWriter::new(send))
//!     );
//!
//!     worker.dataflow::<u64,_,_>(|scope2| {
//!         EventReader::<_,u64,_>::new(recv)
//!             .replay_into(scope2)
//!             .inspect(|x| println!("replayed: {:?}", x));
//!     })
//! }).unwrap();
//! ```

use std::rc::Rc;
use std::cell::RefCell;

use std::io::Write;
use std::ops::DerefMut;

use ::Data;
use dataflow::{Scope, Stream};
use dataflow::channels::pact::{ParallelizationContract, Pipeline};
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pushers::Tee;

use progress::ChangeBatch;
use progress::nested::subgraph::{Source, Target};
use progress::{Timestamp, Operate, Antichain};

use abomonation::Abomonation;

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
    ///     let handle2 = handle1.clone();
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
    ///         EventReader::<_,u64,_>::new(recv)
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

/// Data and progres events of the captured stream.
#[derive(Debug)]
pub enum Event<T, D> {
    /// An initial marker, used only to start the linked list implementation.
    Start,
    /// Progress received via `push_external_progress`.
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream.
    Messages(T, Vec<D>),
}

/// Supports extracting a sequence of timestamp and data.
pub trait Extract<T: Ord, D: Ord> {
    /// Converts `self` into a sequence of timestamped data.
    /// 
    /// Currently this is only implemented for `Receiver<Event<T, D>>`, and is used only
    /// to easily pull data out of a timely dataflow computation once it has completed.
    ///
    /// #Examples
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
    ///     let handle2 = handle1.clone();
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
    fn extract(self) -> Vec<(T, Vec<D>)>;
}

impl<T: Ord, D: Ord> Extract<T,D> for ::std::sync::mpsc::Receiver<Event<T, D>> {
    fn extract(self) -> Vec<(T, Vec<D>)> {
        let mut result = Vec::new();
        for event in self {
            if let Event::Messages(time, data) = event {
                result.push((time, data));
            }
        }
        result.sort_by(|x,y| x.0.cmp(&y.0));

        let mut current = 0;
        for i in 1 .. result.len() {
            if result[current].0 == result[i].0 {
                let dataz = ::std::mem::replace(&mut result[i].1, Vec::new());
                result[current].1.extend(dataz);
            }
            else {
                current = i;
            }
        }

        for &mut (_, ref mut data) in &mut result {
            data.sort();
        }
        result.retain(|x| !x.1.is_empty());
        result
    }
}

impl<T: Abomonation, D: Abomonation> Abomonation for Event<T,D> {
    #[inline] unsafe fn embalm(&mut self) {
        if let Event::Progress(ref mut vec) = *self { vec.embalm(); }
        if let Event::Messages(ref mut time, ref mut data) = *self { time.embalm(); data.embalm(); }
    }
    #[inline] unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        if let Event::Progress(ref vec) = *self { vec.entomb(bytes); }
        if let Event::Messages(ref time, ref data) = *self { time.entomb(bytes); data.entomb(bytes); }
    }
    #[inline] unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut[u8]) -> Option<&'b mut [u8]> {
        match *self {
            Event::Progress(ref mut vec) => { vec.exhume(bytes) },
            Event::Messages(ref mut time, ref mut data) => {
                let temp = bytes; bytes = if let Some(bytes) = time.exhume(temp) { bytes } else { return None; };
                let temp = bytes; bytes = if let Some(bytes) = data.exhume(temp) { bytes } else { return None; };
                Some(bytes)
            }
            Event::Start => Some(bytes),
        }
    }
}


/// A linked list of Event<T, D>.
pub struct EventLink<T, D> {
    /// An event.
    pub event: Event<T, D>,
    /// The next event, if it exists.
    pub next: RefCell<Option<Rc<EventLink<T, D>>>>,
}

impl<T, D> EventLink<T, D> { 
    /// Allocates a new `EventLink`.
    /// 
    /// This could be improved with the knowledge that the first event should always be a frontier(default).
    /// We could, in principle, remove the `Event::Start` event, and thereby no longer have to explain it.
    pub fn new() -> EventLink<T, D> { EventLink { event: Event::Start, next: RefCell::new(None) }}}

/// Iterates over contained `Event<T, D>`.
pub trait EventIterator<T, D> {
    /// Iterates over references to `Event<T, D>` elements.
    fn next(&mut self) -> Option<&Event<T, D>>;
}

/// Receives `Event<T, D>` events.
pub trait EventPusher<T, D> {
    /// Provides a new `Event<T, D>` to the pusher.
    fn push(&mut self, event: Event<T, D>);
}


// implementation for the linked list behind a `Handle`.
impl<T, D> EventPusher<T, D> for ::std::sync::mpsc::Sender<Event<T, D>> {
    fn push(&mut self, event: Event<T, D>) {
        self.send(event).unwrap();
    }
}

// implementation for the linked list behind a `Handle`.
impl<T, D> EventPusher<T, D> for Rc<EventLink<T, D>> {
    fn push(&mut self, event: Event<T, D>) {
        *self.next.borrow_mut() = Some(Rc::new(EventLink { event: event, next: RefCell::new(None) }));
        let next = self.next.borrow().as_ref().unwrap().clone();
        *self = next;
    }
}

impl<T, D> EventIterator<T, D> for Rc<EventLink<T, D>> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let is_some = self.next.borrow().is_some();
        if is_some {
            let next = self.next.borrow().as_ref().unwrap().clone();
            *self = next;
            Some(&self.event)
        }
        else {
            None
        }
    }
}

/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct EventWriter<T, D, W: ::std::io::Write> {
    buffer: Vec<u8>,
    stream: W,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<T, D, W: ::std::io::Write> EventWriter<T, D, W> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(w: W) -> EventWriter<T, D, W> {
        EventWriter {
            buffer: vec![],
            stream: w,
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation, W: ::std::io::Write> EventPusher<T, D> for EventWriter<T, D, W> {
    fn push(&mut self, event: Event<T, D>) {
        unsafe { ::abomonation::encode(&event, &mut self.buffer); }
        self.stream.write_all(&self.buffer[..]).unwrap();
        self.buffer.clear();
    }
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventReader<T, D, R: ::std::io::Read> {
    reader: R,
    bytes: Vec<u8>,
    buff1: Vec<u8>,
    buff2: Vec<u8>,
    consumed: usize,
    valid: usize,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<T, D, R: ::std::io::Read> EventReader<T, D, R> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(r: R) -> EventReader<T, D, R> {
        EventReader {
            reader: r,
            bytes: vec![0u8; 1 << 20],
            buff1: vec![],
            buff2: vec![],
            consumed: 0,
            valid: 0,
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation, R: ::std::io::Read> EventIterator<T, D> for EventReader<T, D, R> {
    fn next(&mut self) -> Option<&Event<T, D>> {

        // if we can decode something, we should just return it! :D
        if unsafe { ::abomonation::decode::<Event<T,D>>(&mut self.buff1[self.consumed..]) }.is_some() {
            let (item, rest) = unsafe { ::abomonation::decode::<Event<T,D>>(&mut self.buff1[self.consumed..]) }.unwrap();
            self.consumed = self.valid - rest.len();
            return Some(item);
        }
        // if we exhaust data we should shift back (if any shifting to do)
        if self.consumed > 0 {
            self.buff2.clear();
            self.buff2.write_all(&self.buff1[self.consumed..]).unwrap();
            ::std::mem::swap(&mut self.buff1, &mut self.buff2);
            self.valid = self.buff1.len();
            self.consumed = 0;
        }

        if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
            self.buff1.write_all(&self.bytes[..len]).unwrap();
            self.valid = self.buff1.len();
        }

        None
    }
}

/// Replay a capture stream into a scope with the same timestamp.
pub trait Replay<T: Timestamp, D: Data> {
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    fn replay_into<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>;
}

// impl<T: Timestamp, D: Data, I: EventIterator<T, D>+'static> Replay<T, D> for I {
//     fn replay_into<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>{
//        let (targets, registrar) = Tee::<S::Timestamp, D>::new();
//        let operator = ReplayOperator {
//            peers: scope.peers(),
//            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(ChangeBatch::new())))),
//            events: self,
//        };

//        let index = scope.add_operator(operator);
//        Stream::new(Source { index: index, port: 0 }, registrar, scope.clone())
//    }
// }

impl<T: Timestamp, D: Data, I> Replay<T, D> for I
where I : IntoIterator,
      <I as IntoIterator>::Item: EventIterator<T, D>+'static {
    fn replay_into<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>{
       let (targets, registrar) = Tee::<S::Timestamp, D>::new();
       let operator = ReplayOperator {
           peers: scope.peers(),
           started: false,
           output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(ChangeBatch::new())))),
           event_streams: self.into_iter().collect(),
       };

       let index = scope.add_operator(operator);
       Stream::new(Source { index: index, port: 0 }, registrar, scope.clone())
   }
}

struct CaptureOperator<T: Timestamp, D: Data, P: EventPusher<T, D>> {
    input: PullCounter<T, D>,
    events: P,
}

impl<T:Timestamp, D: Data, P: EventPusher<T, D>> CaptureOperator<T, D, P> {
    fn new(input: PullCounter<T, D>, events: P) -> CaptureOperator<T, D, P> {
        // events.push(Event::Progress(vec![(Default::default(), 1)]));
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
        let mut map = counts[0].clone();
        map.update(Default::default(), -1);
        if !map.is_empty() {
            self.events.push(Event::Progress(map.into_inner()));
        }
        counts[0].clear();
    }

    // each change to the frontier should be shared
    fn push_external_progress(&mut self, counts: &mut [ChangeBatch<T>]) {
        self.events.push(Event::Progress(counts[0].clone().into_inner()));
        counts[0].clear();
    }

    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<T>],  _: &mut [ChangeBatch<T>], _: &mut [ChangeBatch<T>]) -> bool {
        while let Some((time, data)) = self.input.next() {
            self.events.push(Event::Messages(time.clone(), data.deref_mut().clone()));
        }
        self.input.pull_progress(&mut consumed[0]);
        false
    }
}

struct ReplayOperator<T:Timestamp, D: Data, I: EventIterator<T, D>> {
    peers: usize,
    started: bool,
    event_streams: Vec<I>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
}

impl<T:Timestamp, D: Data, I: EventIterator<T, D>> Operate<T> for ReplayOperator<T, D, I> {
    fn name(&self) -> String { "Replay".to_owned() }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {

        // This initial configuration assumes the worst about the stream, but the first progress indication
        // will amend initial capabilities. In principle, we could try and read some first few event messages,
        // but we want to avoid blocking graph construction.

        let mut result = ChangeBatch::new();
        for _ in 0 .. self.peers {
            result.update(Default::default(), 1);
        }

        (vec![], vec![result])
    }

    fn pull_internal_progress(&mut self, _: &mut [ChangeBatch<T>], internal: &mut [ChangeBatch<T>], produced: &mut [ChangeBatch<T>]) -> bool {

        if !self.started {
            // The first thing we do is modify our capabilities to match the number of streams we manage.
            // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
            // our very first action.
            internal[0].update(Default::default(), - (self.peers as i64));
            internal[0].update(Default::default(), (self.event_streams.len() as i64) - 1);
            self.started = true;
        }

        for event_stream in self.event_streams.iter_mut() {
            while let Some(event) = event_stream.next() {
                match *event {
                    Event::Start => { },
                    Event::Progress(ref vec) => {
                        internal[0].extend(vec.iter().cloned());
                    },
                    Event::Messages(ref time, ref data) => {
                        self.output.session(time).give_iterator(data.iter().cloned());
                    }
                }
            }
        }

        self.output.cease();
        self.output.inner().pull_progress(&mut produced[0]);

        false
    }
}
