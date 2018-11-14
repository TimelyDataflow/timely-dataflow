//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use std::rc::Rc;
use std::cell::RefCell;

use allocator::thread::ThreadBuilder;
use allocator::process::ProcessBuilder as TypedProcessBuilder;
use allocator::{Allocate, AllocateBuilder, Message, Thread, Process};
use allocator::zero_copy::allocator_process::{ProcessBuilder, ProcessAllocator};
use allocator::zero_copy::allocator::{TcpBuilder, TcpAllocator};

use {Push, Pull, Data};

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Generic {
    /// Intra-thread allocator.
    Thread(Thread),
    /// Inter-thread, intra-process allocator.
    Process(Process),
    /// Inter-thread, intra-process serializing allocator.
    ProcessBinary(ProcessAllocator),
    /// Inter-process allocator.
    ZeroCopy(TcpAllocator<Process>),
}

impl Generic {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.index(),
            &Generic::Process(ref p) => p.index(),
            &Generic::ProcessBinary(ref pb) => pb.index(),
            &Generic::ZeroCopy(ref z) => z.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.peers(),
            &Generic::Process(ref p) => p.peers(),
            &Generic::ProcessBinary(ref pb) => pb.peers(),
            &Generic::ZeroCopy(ref z) => z.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {
        match self {
            &mut Generic::Thread(ref mut t) => t.allocate(identifier),
            &mut Generic::Process(ref mut p) => p.allocate(identifier),
            &mut Generic::ProcessBinary(ref mut pb) => pb.allocate(identifier),
            &mut Generic::ZeroCopy(ref mut z) => z.allocate(identifier),
        }
    }
    /// Perform work before scheduling operators.
    fn receive(&mut self, action: impl Fn(&[(usize,i64)])) {
        match self {
            &mut Generic::Thread(ref mut t) => t.receive(action),
            &mut Generic::Process(ref mut p) => p.receive(action),
            &mut Generic::ProcessBinary(ref mut pb) => pb.receive(action),
            &mut Generic::ZeroCopy(ref mut z) => z.receive(action),
        }
    }
    /// Perform work after scheduling operators.
    pub fn flush(&mut self) {
        match self {
            &mut Generic::Thread(ref mut t) => t.flush(),
            &mut Generic::Process(ref mut p) => p.flush(),
            &mut Generic::ProcessBinary(ref mut pb) => pb.flush(),
            &mut Generic::ZeroCopy(ref mut z) => z.flush(),
        }
    }
    fn counts(&self) -> &Rc<RefCell<Vec<(usize, i64)>>> {
        match self {
            &Generic::Thread(ref t) => t.counts(),
            &Generic::Process(ref p) => p.counts(),
            &Generic::ProcessBinary(ref pb) => pb.counts(),
            &Generic::ZeroCopy(ref z) => z.counts(),
        }
    }
}

impl Allocate for Generic {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {
        self.allocate(identifier)
    }

    fn receive(&mut self, action: impl Fn(&[(usize,i64)])) { self.receive(action); }
    fn flush(&mut self) { self.flush(); }
    fn counts(&self) -> &Rc<RefCell<Vec<(usize, i64)>>> { self.counts() }
}


/// Enumerations of constructable implementors of `Allocate`.
///
/// The builder variants are meant to be `Send`, so that they can be moved across threads,
/// whereas the allocator they construct may not. As an example, the `ProcessBinary` type
/// contains `Rc` wrapped state, and so cannot itself be moved across threads.
pub enum GenericBuilder {
    /// Builder for `Thread` allocator.
    Thread(ThreadBuilder),
    /// Builder for `Process` allocator.
    Process(TypedProcessBuilder),
    /// Builder for `ProcessBinary` allocator.
    ProcessBinary(ProcessBuilder),
    /// Builder for `ZeroCopy` allocator.
    ZeroCopy(TcpBuilder<TypedProcessBuilder>),
}

impl AllocateBuilder for GenericBuilder {
    type Allocator = Generic;
    fn build(self) -> Generic {
        match self {
            GenericBuilder::Thread(t) => Generic::Thread(t.build()),
            GenericBuilder::Process(p) => Generic::Process(p.build()),
            GenericBuilder::ProcessBinary(pb) => Generic::ProcessBinary(pb.build()),
            GenericBuilder::ZeroCopy(z) => Generic::ZeroCopy(z.build()),
        }
    }
}
