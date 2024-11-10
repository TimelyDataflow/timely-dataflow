//! A simple communication infrastructure providing typed exchange channels.
//!
//! This crate is part of the timely dataflow system, used primarily for its inter-worker communication.
//! It may be independently useful, but it is separated out mostly to make clear boundaries in the project.
//!
//! Threads are spawned with an [`allocator::Generic`](allocator::generic::Generic), whose
//! [`allocate`](Allocate::allocate) method returns a pair of several send endpoints and one
//! receive endpoint. Messages sent into a send endpoint will eventually be received by the corresponding worker,
//! if it receives often enough. The point-to-point channels are each FIFO, but with no fairness guarantees.
//!
//! To be communicated, a type must implement the [`Bytesable`] trait.
//!
//! Channel endpoints also implement a lower-level `push` and `pull` interface (through the [`Push`] and [`Pull`]
//! traits), which is used for more precise control of resources.
//!
//! # Examples
//! ```
//! use timely_communication::{Allocate, Bytesable};
//! 
//! /// A wrapper that indicates `bincode` as the serialization/deserialization strategy.
//! pub struct Message {
//!     /// Text contents.
//!     pub payload: String,
//! }
//! 
//! impl Bytesable for Message {
//!     fn from_bytes(bytes: timely_bytes::arc::Bytes) -> Self {
//!         Message { payload: std::str::from_utf8(&bytes[..]).unwrap().to_string() }
//!     }
//! 
//!     fn length_in_bytes(&self) -> usize {
//!         self.payload.len()
//!     }
//! 
//!     fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
//!         writer.write_all(self.payload.as_bytes()).unwrap();
//!     }
//! }
//! 
//! fn main() {
//! 
//!     // extract the configuration from user-supplied arguments, initialize the computation.
//!     let config = timely_communication::Config::from_args(std::env::args()).unwrap();
//!     let guards = timely_communication::initialize(config, |mut allocator| {
//! 
//!         println!("worker {} of {} started", allocator.index(), allocator.peers());
//! 
//!         // allocates a pair of senders list and one receiver.
//!         let (mut senders, mut receiver) = allocator.allocate(0);
//! 
//!         // send typed data along each channel
//!         for i in 0 .. allocator.peers() {
//!             senders[i].send(Message { payload: format!("hello, {}", i)});
//!             senders[i].done();
//!         }
//! 
//!         // no support for termination notification,
//!         // we have to count down ourselves.
//!         let mut received = 0;
//!         while received < allocator.peers() {
//! 
//!             allocator.receive();
//! 
//!             if let Some(message) = receiver.recv() {
//!                 println!("worker {}: received: <{}>", allocator.index(), message.payload);
//!                 received += 1;
//!             }
//! 
//!             allocator.release();
//!         }
//! 
//!         allocator.index()
//!     });
//! 
//!     // computation runs until guards are joined or dropped.
//!     if let Ok(guards) = guards {
//!         for guard in guards.join() {
//!             println!("result: {:?}", guard);
//!         }
//!     }
//!     else { println!("error in computation"); }
//! }
//! ```
//!
//! This should produce output like:
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

pub mod allocator;
pub mod networking;
pub mod initialize;
pub mod logging;
pub mod buzzer;

pub use allocator::Generic as Allocator;
pub use allocator::Allocate;
pub use initialize::{initialize, initialize_from, Config, WorkerGuards};

use timely_bytes::arc::Bytes;

/// A type that can be serialized and deserialized through `Bytes`.
pub trait Bytesable {
    /// Wrap bytes as `Self`.
    fn from_bytes(bytes: Bytes) -> Self;

    /// The number of bytes required to serialize the data.
    fn length_in_bytes(&self) -> usize;

    /// Writes the binary representation into `writer`.
    fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W);
}

/// Pushing elements of type `T`.
///
/// This trait moves data around using references rather than ownership,
/// which provides the opportunity for zero-copy operation. In the call
/// to `push(element)` the implementor can *swap* some other value to
/// replace `element`, effectively returning the value to the caller.
///
/// Conventionally, a sequence of calls to `push()` should conclude with
/// a call of `push(&mut None)` or `done()` to signal to implementors that
/// another call to `push()` may not be coming.
pub trait Push<T> {
    /// Pushes `element` with the opportunity to take ownership.
    fn push(&mut self, element: &mut Option<T>);
    /// Pushes `element` and drops any resulting resources.
    #[inline]
    fn send(&mut self, element: T) { self.push(&mut Some(element)); }
    /// Pushes `None`, conventionally signalling a flush.
    #[inline]
    fn done(&mut self) { self.push(&mut None); }
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) { (**self).push(element) }
}

/// Pulling elements of type `T`.
pub trait Pull<T> {
    /// Pulls an element and provides the opportunity to take ownership.
    ///
    /// The puller may mutate the result, in particular take ownership of the data by
    /// replacing it with other data or even `None`. This allows the puller to return
    /// resources to the implementor.
    ///
    /// If `pull` returns `None` this conventionally signals that no more data is available
    /// at the moment, and the puller should find something better to do.
    fn pull(&mut self) -> &mut Option<T>;
    /// Takes an `Option<T>` and leaves `None` behind.
    #[inline]
    fn recv(&mut self) -> Option<T> { self.pull().take() }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> { (**self).pull() }
}


use crossbeam_channel::{Sender, Receiver};

/// Allocate a matrix of send and receive changes to exchange items.
///
/// This method constructs channels for `sends` threads to create and send
/// items of type `T` to `recvs` receiver threads.
fn promise_futures<T>(sends: usize, recvs: usize) -> (Vec<Vec<Sender<T>>>, Vec<Vec<Receiver<T>>>) {

    // each pair of workers has a sender and a receiver.
    let mut senders: Vec<_> = (0 .. sends).map(|_| Vec::with_capacity(recvs)).collect();
    let mut recvers: Vec<_> = (0 .. recvs).map(|_| Vec::with_capacity(sends)).collect();

    for sender in 0 .. sends {
        for recver in 0 .. recvs {
            let (send, recv) = crossbeam_channel::unbounded();
            senders[sender].push(send);
            recvers[recver].push(recv);
        }
    }

    (senders, recvers)
}
