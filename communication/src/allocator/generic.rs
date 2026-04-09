//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use std::rc::Rc;
use std::cell::RefCell;

use crate::allocator::thread::ThreadBuilder;
use crate::allocator::process::ProcessBuilder as TypedProcessBuilder;
use crate::allocator::{Allocate, AllocateBuilder, Exchangeable, Thread, Process};
use crate::allocator::zero_copy::allocator_process::{ProcessBuilder, ProcessAllocator};
use crate::allocator::zero_copy::allocator::{TcpBuilder, TcpAllocator};

use crate::{Push, Pull};

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Allocator {
    /// Intra-thread allocator.
    Thread(Thread),
    /// Inter-thread, intra-process allocator.
    Process(Process),
    /// Inter-thread, intra-process serializing allocator.
    ProcessBinary(ProcessAllocator),
    /// Inter-process allocator.
    ZeroCopy(TcpAllocator<Process>),
    /// Inter-process allocator, intra-process serializing allocator.
    ZeroCopyBinary(TcpAllocator<ProcessAllocator>),
}

impl Allocator {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            Allocator::Thread(t) => t.index(),
            Allocator::Process(p) => p.index(),
            Allocator::ProcessBinary(pb) => pb.index(),
            Allocator::ZeroCopy(z) => z.index(),
            Allocator::ZeroCopyBinary(z) => z.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            Allocator::Thread(t) => t.peers(),
            Allocator::Process(p) => p.peers(),
            Allocator::ProcessBinary(pb) => pb.peers(),
            Allocator::ZeroCopy(z) => z.peers(),
            Allocator::ZeroCopyBinary(z) => z.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Exchangeable>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {
        match self {
            Allocator::Thread(t) => t.allocate(identifier),
            Allocator::Process(p) => p.allocate(identifier),
            Allocator::ProcessBinary(pb) => pb.allocate(identifier),
            Allocator::ZeroCopy(z) => z.allocate(identifier),
            Allocator::ZeroCopyBinary(z) => z.allocate(identifier),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    fn broadcast<T: Exchangeable+Clone>(&mut self, identifier: usize) -> (Box<dyn Push<T>>, Box<dyn Pull<T>>) {
        match self {
            Allocator::Thread(t) => t.broadcast(identifier),
            Allocator::Process(p) => p.broadcast(identifier),
            Allocator::ProcessBinary(pb) => pb.broadcast(identifier),
            Allocator::ZeroCopy(z) => z.broadcast(identifier),
            Allocator::ZeroCopyBinary(z) => z.broadcast(identifier),
        }
    }
    /// Perform work before scheduling operators.
    fn receive(&mut self) {
        match self {
            Allocator::Thread(t) => t.receive(),
            Allocator::Process(p) => p.receive(),
            Allocator::ProcessBinary(pb) => pb.receive(),
            Allocator::ZeroCopy(z) => z.receive(),
            Allocator::ZeroCopyBinary(z) => z.receive(),
        }
    }
    /// Perform work after scheduling operators.
    pub fn release(&mut self) {
        match self {
            Allocator::Thread(t) => t.release(),
            Allocator::Process(p) => p.release(),
            Allocator::ProcessBinary(pb) => pb.release(),
            Allocator::ZeroCopy(z) => z.release(),
            Allocator::ZeroCopyBinary(z) => z.release(),
        }
    }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        match self {
            Allocator::Thread(ref t) => t.events(),
            Allocator::Process(ref p) => p.events(),
            Allocator::ProcessBinary(ref pb) => pb.events(),
            Allocator::ZeroCopy(ref z) => z.events(),
            Allocator::ZeroCopyBinary(ref z) => z.events(),
        }
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
            Allocator::ProcessBinary(pb) => pb.await_events(_duration),
            Allocator::ZeroCopy(z) => z.await_events(_duration),
            Allocator::ZeroCopyBinary(z) => z.await_events(_duration),
        }
    }
}


/// Enumerations of constructable implementors of `Allocate`.
///
/// The builder variants are meant to be `Send`, so that they can be moved across threads,
/// whereas the allocator they construct may not. As an example, the `ProcessBinary` type
/// contains `Rc` wrapped state, and so cannot itself be moved across threads.
pub enum AllocatorBuilder {
    /// Builder for `Thread` allocator.
    Thread(ThreadBuilder),
    /// Builder for `Process` allocator.
    Process(TypedProcessBuilder),
    /// Builder for `ProcessBinary` allocator.
    ProcessBinary(ProcessBuilder),
    /// Builder for `ZeroCopy` allocator.
    ZeroCopy(TcpBuilder<TypedProcessBuilder>),
    /// Builder for `ZeroCopyBinary` allocator.
    ZeroCopyBinary(TcpBuilder<ProcessBuilder>),
}

impl AllocateBuilder for AllocatorBuilder {
    type Allocator = Allocator;
    fn build(self) -> Allocator {
        match self {
            AllocatorBuilder::Thread(t) => Allocator::Thread(t.build()),
            AllocatorBuilder::Process(p) => Allocator::Process(p.build()),
            AllocatorBuilder::ProcessBinary(pb) => Allocator::ProcessBinary(pb.build()),
            AllocatorBuilder::ZeroCopy(z) => Allocator::ZeroCopy(z.build()),
            AllocatorBuilder::ZeroCopyBinary(z) => Allocator::ZeroCopyBinary(z.build()),
        }
    }
}
