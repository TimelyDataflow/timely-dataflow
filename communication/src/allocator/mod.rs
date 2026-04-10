//! Types and traits for the allocation of channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;

pub use self::thread::Thread;
pub use self::generic::{Allocator, AllocatorBuilder};

pub mod thread;
pub mod process;
pub mod generic;

pub mod canary;
pub mod counters;

pub mod zero_copy;

use crate::{Bytesable, Push, Pull};
use crate::allocator::process::{Process as TypedProcess, ProcessBuilder as TypedProcessBuilder};
use crate::allocator::zero_copy::allocator_process::{ProcessAllocator as BytesProcess, ProcessBuilder as BytesProcessBuilder};

/// A proto-allocator, which implements `Send` and can be completed with `build`.
///
/// This trait exists because some allocators contain elements that do not implement
/// the `Send` trait, for example `Rc` wrappers for shared state. As such, what we
/// actually need to create to initialize a computation are builders, which we can
/// then move into new threads each of which then construct their actual allocator.
pub(crate) trait AllocateBuilder : Send {
    /// The type of allocator to be built.
    type Allocator: Allocate;
    /// Builds allocator, consumes self.
    fn build(self) -> Self::Allocator;
}

use std::any::Any;

/// A type that can be sent along an allocated channel.
pub trait Exchangeable : Send+Any+Bytesable { }
impl<T: Send+Any+Bytesable> Exchangeable for T { }

/// A type capable of allocating channels.
///
/// There is some feature creep, in that this contains several convenience methods about the nature
/// of the allocated channels, and maintenance methods to ensure that they move records around.
pub(crate) trait Allocate {
    /// The index of the worker out of `(0..self.peers())`.
    fn index(&self) -> usize;
    /// The number of workers in the communication group.
    fn peers(&self) -> usize;
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Exchangeable>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>);
    /// A shared queue of communication events with channel identifier.
    ///
    /// It is expected that users of the channel allocator will regularly
    /// drain these events in order to drive their computation. If they
    /// fail to do so the event queue may become quite large, and turn
    /// into a performance problem.
    fn events(&self) -> &Rc<RefCell<Vec<usize>>>;

    /// Awaits communication events.
    ///
    /// This method may park the current thread, for at most `duration`,
    /// until new events arrive.
    /// The method is not guaranteed to wait for any amount of time, but
    /// good implementations should use this as a hint to park the thread.
    fn await_events(&self, _duration: Option<Duration>) { }

    /// Ensure that received messages are surfaced in each channel.
    ///
    /// This method should be called to ensure that received messages are
    /// surfaced in each channel, but failing to call the method does not
    /// ensure that they are not surfaced.
    ///
    /// Generally, this method is the indication that the allocator should
    /// present messages contained in otherwise scarce resources (for example
    /// network buffers), under the premise that someone is about to consume
    /// the messages and release the resources.
    fn receive(&mut self) { }

    /// Signal the completion of a batch of reads from channels.
    ///
    /// Conventionally, this method signals to the communication fabric
    /// that the worker is taking a break from reading from channels, and
    /// the fabric should consider re-acquiring scarce resources. This can
    /// lead to the fabric performing defensive copies out of un-consumed
    /// buffers, and can be a performance problem if invoked casually.
    fn release(&mut self) { }

    /// Allocates a broadcast channel, where each pushed message is received by all.
    fn broadcast<T: Exchangeable + Clone>(&mut self, identifier: usize) -> (Box<dyn Push<T>>, Box<dyn Pull<T>>) {
        let (pushers, pull) = self.allocate(identifier);
        (Box::new(Broadcaster { spare: None, pushers }), pull)
    }
}

/// An adapter to broadcast any pushed element.
struct Broadcaster<T> {
    /// Spare element for defensive copies.
    spare: Option<T>,
    /// Destinations to which pushed elements should be broadcast.
    pushers: Vec<Box<dyn Push<T>>>,
}

impl<T: Clone> Push<T> for Broadcaster<T> {
    fn push(&mut self, element: &mut Option<T>) {
        // Push defensive copies to pushers after the first.
        for pusher in self.pushers.iter_mut().skip(1) {
            self.spare.clone_from(element);
            pusher.push(&mut self.spare);
        }
        // Push the element itself at the first pusher.
        for pusher in self.pushers.iter_mut().take(1) {
            pusher.push(element);
        }
    }
}

use crate::allocator::zero_copy::bytes_slab::BytesRefill;

/// A builder for vectors of peers.
pub(crate) trait PeerBuilder {
    /// The peer type.
    type Peer: AllocateBuilder + Sized;
    /// Allocate a list of `Self::Peer` of length `peers`.
    fn new_vector(peers: usize, refill: BytesRefill) -> Vec<Self::Peer>;
}


/// Two flavors of intra-process allocator builder.
#[non_exhaustive]
pub enum ProcessBuilder {
    /// Regular intra-process allocator (mpsc-based).
    Typed(TypedProcessBuilder),
    /// Binary intra-process allocator (zero-copy serialized).
    Bytes(BytesProcessBuilder),
}

impl ProcessBuilder {
    /// Builds the runtime allocator from this builder.
    pub fn build(self) -> Process {
        match self {
            ProcessBuilder::Typed(t) => Process::Typed(t.build()),
            ProcessBuilder::Bytes(b) => Process::Bytes(b.build()),
        }
    }

    /// Constructs a vector of regular (mpsc-based, "Typed") intra-process builders.
    pub fn new_typed_vector(peers: usize, refill: BytesRefill) -> Vec<Self> {
        <TypedProcess as PeerBuilder>::new_vector(peers, refill)
            .into_iter()
            .map(ProcessBuilder::Typed)
            .collect()
    }

    /// Constructs a vector of binary (zero-copy serialized, "Bytes") intra-process builders.
    pub fn new_bytes_vector(peers: usize, refill: BytesRefill) -> Vec<Self> {
        <BytesProcessBuilder as PeerBuilder>::new_vector(peers, refill)
            .into_iter()
            .map(ProcessBuilder::Bytes)
            .collect()
    }
}

/// The runtime counterpart of `ProcessBuilder`: the actual constructed inner allocator.
///
/// Inherent methods mirror the subset of `Allocate` that `TcpAllocator` needs from its inner.
#[non_exhaustive]
pub enum Process {
    /// Regular intra-process allocator.
    Typed(TypedProcess),
    /// Binary intra-process allocator.
    Bytes(BytesProcess),
}

impl Process {
    pub(crate) fn index(&self) -> usize {
        match self {
            Process::Typed(p) => p.index(),
            Process::Bytes(pb) => pb.index(),
        }
    }
    pub(crate) fn peers(&self) -> usize {
        match self {
            Process::Typed(p) => p.peers(),
            Process::Bytes(pb) => pb.peers(),
        }
    }
    pub(crate) fn allocate<T: Exchangeable>(&mut self, identifier: usize)
        -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>)
    {
        match self {
            Process::Typed(p) => p.allocate(identifier),
            Process::Bytes(pb) => pb.allocate(identifier),
        }
    }
    pub(crate) fn broadcast<T: Exchangeable + Clone>(&mut self, identifier: usize)
        -> (Box<dyn Push<T>>, Box<dyn Pull<T>>)
    {
        match self {
            Process::Typed(p) => p.broadcast(identifier),
            Process::Bytes(pb) => pb.broadcast(identifier),
        }
    }
    pub(crate) fn receive(&mut self) {
        match self {
            Process::Typed(p) => p.receive(),
            Process::Bytes(pb) => pb.receive(),
        }
    }
    pub(crate) fn release(&mut self) {
        match self {
            Process::Typed(p) => p.release(),
            Process::Bytes(pb) => pb.release(),
        }
    }
    pub(crate) fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        match self {
            Process::Typed(p) => p.events(),
            Process::Bytes(pb) => pb.events(),
        }
    }
    pub(crate) fn await_events(&self, duration: Option<std::time::Duration>) {
        match self {
            Process::Typed(p) => p.await_events(duration),
            Process::Bytes(pb) => pb.await_events(duration),
        }
    }
}
