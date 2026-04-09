//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use std::rc::Rc;
use std::cell::RefCell;

use crate::allocator::thread::ThreadBuilder;
use crate::allocator::{Allocate, AllocateBuilder, Exchangeable, Thread, Process, ProcessBuilder};
use crate::allocator::zero_copy::allocator::{TcpBuilder, TcpAllocator};

use crate::{Push, Pull};

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Allocator {
    /// Intra-thread allocator.
    Thread(Thread),
    /// Inter-thread, intra-process allocator (in either of two flavors, see `Process`).
    Process(Process),
    /// Inter-process allocator (TCP-based, with a `Process` as its intra-process inner).
    Tcp(TcpAllocator),
}

impl Allocator {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            Allocator::Thread(t) => t.index(),
            Allocator::Process(p) => p.index(),
            Allocator::Tcp(z) => z.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            Allocator::Thread(t) => t.peers(),
            Allocator::Process(p) => p.peers(),
            Allocator::Tcp(z) => z.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    pub fn allocate<T: Exchangeable>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {
        match self {
            Allocator::Thread(t) => t.allocate(identifier),
            Allocator::Process(p) => p.allocate(identifier),
            Allocator::Tcp(z) => z.allocate(identifier),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    pub fn broadcast<T: Exchangeable+Clone>(&mut self, identifier: usize) -> (Box<dyn Push<T>>, Box<dyn Pull<T>>) {
        match self {
            Allocator::Thread(t) => t.broadcast(identifier),
            Allocator::Process(p) => p.broadcast(identifier),
            Allocator::Tcp(z) => z.broadcast(identifier),
        }
    }
    /// Perform work before scheduling operators.
    pub fn receive(&mut self) {
        match self {
            Allocator::Thread(t) => t.receive(),
            Allocator::Process(p) => p.receive(),
            Allocator::Tcp(z) => z.receive(),
        }
    }
    /// Perform work after scheduling operators.
    pub fn release(&mut self) {
        match self {
            Allocator::Thread(t) => t.release(),
            Allocator::Process(p) => p.release(),
            Allocator::Tcp(z) => z.release(),
        }
    }
    /// Provides access to the shared event queue.
    pub fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        match self {
            Allocator::Thread(ref t) => t.events(),
            Allocator::Process(ref p) => p.events(),
            Allocator::Tcp(ref z) => z.events(),
        }
    }

    /// Awaits communication events.
    pub fn await_events(&self, duration: Option<std::time::Duration>) {
        match self {
            Allocator::Thread(t) => t.await_events(duration),
            Allocator::Process(p) => p.await_events(duration),
            Allocator::Tcp(z) => z.await_events(duration),
        }
    }

    /// Constructs a pipeline channel from the worker to itself.
    ///
    /// By default, this method uses the thread-local channel constructor
    /// based on a shared `VecDeque` which updates the event queue.
    pub fn pipeline<T: 'static>(&mut self, identifier: usize) ->
        (crate::allocator::thread::ThreadPusher<T>,
         crate::allocator::thread::ThreadPuller<T>)
    {
        crate::allocator::thread::Thread::new_from(identifier, Rc::clone(self.events()))
    }
}

impl Allocate for Allocator {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Exchangeable>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {
        self.allocate(identifier)
    }
    fn broadcast<T: Exchangeable+Clone>(&mut self, identifier: usize) -> (Box<dyn Push<T>>, Box<dyn Pull<T>>) {
        self.broadcast(identifier)
    }
    fn receive(&mut self) { self.receive(); }
    fn release(&mut self) { self.release(); }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> { self.events() }
    fn await_events(&self, _duration: Option<std::time::Duration>) {
        match self {
            Allocator::Thread(t) => t.await_events(_duration),
            Allocator::Process(p) => p.await_events(_duration),
            Allocator::Tcp(z) => z.await_events(_duration),
        }
    }
}


/// Enumerations of constructable implementors of `Allocate`.
///
/// The builder variants are meant to be `Send`, so that they can be moved across threads,
/// whereas the allocator they construct may not. As an example, the binary `Process` type
/// contains `Rc` wrapped state, and so cannot itself be moved across threads.
pub enum AllocatorBuilder {
    /// Builder for the `Thread` allocator.
    Thread(ThreadBuilder),
    /// Builder for a `Process` allocator (in either of two flavors, see `ProcessBuilder`).
    Process(ProcessBuilder),
    /// Builder for the `Tcp` (inter-process) allocator.
    Tcp(TcpBuilder),
}

impl AllocateBuilder for AllocatorBuilder {
    type Allocator = Allocator;
    fn build(self) -> Allocator {
        match self {
            AllocatorBuilder::Thread(t) => Allocator::Thread(t.build()),
            AllocatorBuilder::Process(p) => Allocator::Process(p.build()),
            AllocatorBuilder::Tcp(z) => Allocator::Tcp(z.build()),
        }
    }
}
