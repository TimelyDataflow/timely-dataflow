//! A simple communication infrastructure providing typed exchange channels.
//!
//! This crate is part of the timely dataflow system, used primarily for its inter-worker communication.
//! It may be independently useful, but it is separated out mostly to make clear boundaries in the project.
//!
//! Threads are spawned with an [`allocator::Generic`](./allocator/generic/enum.Generic.html), whose `allocate` method returns a pair of several send endpoints and one
//! receive endpoint. Messages sent into a send endpoint will eventually be received by the corresponding worker,
//! if it receives often enough. The point-to-point channels are each FIFO, but with no fairness guarantees.
//!
//! To be communicated, a type must implement the [`Serialize`](./trait.Serialize.html) trait. A default implementation of `Serialize` is
//! provided for any type implementing [`Abomonation`](../abomonation/trait.Abomonation.html). To implement other serialization strategies, wrap your type
//! and implement `Serialize` for your wrapper.
//!
//! Channel endpoints also implement a lower-level `push` and `pull` interface (through the [`Push`](./trait.Push.html) and [`Pull`](./trait.Pull.html)
//! traits), which is used for more precise control of resources.
//!
//! #Examples
//! ```
//! // configure for two threads, just one process.
//! let config = timely_communication::Configuration::Process(2);
//!
//! // create a source of inactive loggers.
//! let logger = ::std::sync::Arc::new(|_| timely_communication::logging::BufferingLogger::new_inactive());
//!
//! // initializes communication, spawns workers
//! let guards = timely_communication::initialize(config, logger, |mut allocator| {
//!     println!("worker {} started", allocator.index());
//!
//!     // allocates pair of senders list and one receiver.
//!     let (mut senders, mut receiver, _) = allocator.allocate();
//!
//!     // send typed data along each channel
//!     use timely_communication::Message;
//!     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
//!     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
//!
//!     // no support for termination notification,
//!     // we have to count down ourselves.
//!     let mut expecting = 2;
//!     while expecting > 0 {
//!
//!         allocator.pre_work();
//!         if let Some(message) = receiver.recv() {
//!             use std::ops::Deref;
//!             println!("worker {}: received: <{}>", allocator.index(), message.deref());
//!             expecting -= 1;
//!         }
//!         allocator.post_work();
//!     }
//!
//!     // optionally, return something
//!     allocator.index()
//! });
//!
//! // computation runs until guards are joined or dropped.
//! if let Ok(guards) = guards {
//!     for guard in guards.join() {
//!         println!("result: {:?}", guard);
//!     }
//! }
//! else { println!("error in computation"); }
//! ```
//!
//! The should produce output like:
//!
//! ```ignore
//! worker 0 started
//! worker 1 started
//! worker 0: received: <hello, 0>
//! worker 1: received: <hello, 1>
//! worker 0: received: <hello, 0>
//! worker 1: received: <hello, 1>
//! result: Ok(0)
//! result: Ok(1)
//! ```

#![forbid(missing_docs)]

#[cfg(feature = "arg_parse")]
extern crate getopts;
extern crate abomonation;
#[macro_use] extern crate abomonation_derive;
extern crate time;

extern crate bytes;

pub mod allocator;
pub mod networking;
pub mod initialize;
pub mod logging;
pub mod message;

use std::any::Any;

use abomonation::Abomonation;

pub use allocator::Generic as Allocator;
pub use allocator::Allocate;
pub use initialize::{initialize, initialize_from, Configuration, WorkerGuards};
pub use message::Message;

/// A composite trait for types that may be used with channels.
pub trait Data : Send+Any+Abomonation+'static { }
impl<T: Send+Any+Abomonation+'static> Data for T { }

/// Pushing elements of type `T`.
pub trait Push<T> {
    /// Pushes `element` and provides the opportunity to take ownership.
    ///
    /// The value of `element` after the call may be changed. A change does not imply anything other
    /// than that the implementor took resources associated with `element` and is returning other
    /// resources.
    fn push(&mut self, element: &mut Option<T>);
    /// Pushes `element` and drops any resulting resources.
    #[inline(always)]
    fn send(&mut self, element: T) { self.push(&mut Some(element)); }
    /// Pushes `None`, conventionally signalling a flush.
    #[inline(always)]
    fn done(&mut self) { self.push(&mut None); }
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline(always)]
    fn push(&mut self, element: &mut Option<T>) { (**self).push(element) }
}

/// Pulling elements of type `T`.
pub trait Pull<T> {
    /// Pulls an element and provides the opportunity to take ownership.
    ///
    /// The receiver may mutate the result, in particular take ownership of the data by replacing
    /// it with other data or even `None`.
    /// If `pull` returns `None` this conventionally signals that no more data is available
    /// at the moment.
    fn pull(&mut self) -> &mut Option<T>;
    /// Takes an `Option<T>` and leaves `None` behind.
    #[inline(always)]
    fn recv(&mut self) -> Option<T> { self.pull().take() }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<T> { (**self).pull() }
}
