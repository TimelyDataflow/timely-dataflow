//! Traits and types for capturing timely dataflow streams.
//!
//! All timely dataflow streams can be captured, but there are many ways to capture
//! these streams. A stream may be `capture_into`'d any type implementing `EventPusher`,
//! and there are several default implementations, including a linked-list, Rust's MPSC
//! queue, and a binary serializer wrapping any `W: Write`.

use std::rc::Rc;
use std::cell::RefCell;
use std::ops::DerefMut;

use ::Data;
use dataflow::{Scope, Stream};
use dataflow::channels::pact::Pipeline;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::operators::generic::builder_raw::OperatorBuilder;

use progress::ChangeBatch;
use progress::Timestamp;

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
    /// ```rust,ignore
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
    fn capture_into<P: EventPusher<S::Timestamp, D>+'static>(&self, event_pusher: P) {

        let mut builder = OperatorBuilder::new("Capture".to_owned(), self.scope());
        let mut input = PullCounter::new(builder.new_input(self, Pipeline));
        let mut started = false;

        let event_pusher1 = Rc::new(RefCell::new(event_pusher));
        let event_pusher2 = event_pusher1.clone();

        builder.build(
            move |frontier| {
                if !started {
                    frontier[0].update(Default::default(), -1);
                    started = true;
                }
                if !frontier[0].is_empty() {
                    let to_send = ::std::mem::replace(&mut frontier[0], ChangeBatch::new());
                    event_pusher1.borrow_mut().push(Event::Progress(to_send.into_inner()));
                }
            },
            move |consumed, _internal, _external| {
                // turn each received message into an event.
                let borrow = event_pusher2.borrow_mut();
                while let Some((time, data)) = input.next() {
                    borrow.push(Event::Messages(time.clone(), data.deref_mut().clone()));
                }
                input.consumed().borrow_mut().drain_into(&mut consumed[0]);
                false
            }
        );
    }
}
