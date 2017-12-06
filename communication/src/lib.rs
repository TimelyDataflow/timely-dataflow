//! A simple communication infrastructure providing typed exchange channels.
//!
//! This crate is part of the timely dataflow system, used primarily for its inter-worker communication.
//! It may be indepedently useful, but it is separated out mostly to make clear boundaries in the project.
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
//! // initializes communication, spawns workers
//! let guards = timely_communication::initialize(config, |mut allocator| {
//!     println!("worker {} started", allocator.index());
//!
//!     // allocates pair of senders list and one receiver.
//!     let (mut senders, mut receiver) = allocator.allocate();
//!
//!     // send typed data along each channel
//!     senders[0].send(format!("hello, {}", 0));
//!     senders[1].send(format!("hello, {}", 1));
//!
//!     // no support for termination notification,
//!     // we have to count down ourselves.
//!     let mut expecting = 2;
//!     while expecting > 0 {
//!         if let Some(message) = receiver.recv() {
//!             println!("worker {}: received: <{}>", allocator.index(), message);
//!             expecting -= 1;
//!         }
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

extern crate getopts;
extern crate byteorder;
extern crate abomonation;

extern crate timely_logging;

pub mod allocator;
mod networking;
pub mod initialize;
mod drain;
pub mod logging;

use std::any::Any;
use abomonation::{Abomonation, encode, decode};

pub use allocator::Generic as Allocator;
pub use allocator::Allocate;
pub use initialize::{initialize, Configuration, WorkerGuards};

/// A composite trait for types that may be used with channels.
pub trait Data : Send+Any+Serialize+'static { }
impl<T: Send+Any+Serialize+'static> Data for T { }

/// Conversions to and from `Vec<u8>`.
///
/// A type must implement this trait to move along the channels produced by an `A: Allocate`.
///
/// A default implementation is provided for any `T: Abomonation+Clone`.
pub trait Serialize {
    /// Append the binary representation of `self` to a vector of bytes. The `&mut self` argument
    /// may be mutated, but the second argument should only be appended to.
    fn into_bytes(&mut self, &mut Vec<u8>);
    /// Recover an instance of Self from its binary representation. The `&mut Vec<u8>` argument may
    /// be taken with `mem::replace` if it is needed.
    fn from_bytes(&mut Vec<u8>) -> Self;
}

// NOTE : this should be unsafe, because these methods are.
// NOTE : figure this out later. don't use for serious things.
impl<T: Abomonation+Clone> Serialize for T {
    fn into_bytes(&mut self, bytes: &mut Vec<u8>) {
        // NOTE: `unwrap` should be ok, as Rust docs say writes to `Vec<u8>` do not fail.
        unsafe { encode(self, bytes).unwrap(); }
    }
    fn from_bytes(bytes: &mut Vec<u8>) -> Self {
        (* unsafe { decode::<T>(bytes) }.unwrap().0).clone()
    }
}

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
    /// If `pull` returns `None` this conventially signals that no more data is available
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
