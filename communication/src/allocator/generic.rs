//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use allocator::{Allocate, Thread, Process, Binary};
use allocator::process_binary::{ProcessBinary, ProcessBinaryBuilder};
use {Push, Pull, Data};

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Generic {
    Thread(Thread),
    Process(Process),
    Binary(Binary),
    ProcessBinary(ProcessBinary<::allocator::process_binary::vec::VecBytesExchange>),
}

impl Generic {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.index(),
            &Generic::Process(ref p) => p.index(),
            &Generic::Binary(ref b) => b.index(),
            &Generic::ProcessBinary(ref pb) => pb.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.peers(),
            &Generic::Process(ref p) => p.peers(),
            &Generic::Binary(ref b) => b.peers(),
            &Generic::ProcessBinary(ref pb) => pb.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    pub fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) {
        match self {
            &mut Generic::Thread(ref mut t) => t.allocate(),
            &mut Generic::Process(ref mut p) => p.allocate(),
            &mut Generic::Binary(ref mut b) => b.allocate(),
            &mut Generic::ProcessBinary(ref mut pb) => pb.allocate(),
        }
    }

    pub fn pre_work(&mut self) {
        match self {
            &mut Generic::Thread(ref mut t) => t.pre_work(),
            &mut Generic::Process(ref mut p) => p.pre_work(),
            &mut Generic::Binary(ref mut b) => b.pre_work(),
            &mut Generic::ProcessBinary(ref mut pb) => pb.pre_work(),
        }
    }
    pub fn post_work(&mut self) {
        match self {
            &mut Generic::Thread(ref mut t) => t.post_work(),
            &mut Generic::Process(ref mut p) => p.post_work(),
            &mut Generic::Binary(ref mut b) => b.post_work(),
            &mut Generic::ProcessBinary(ref mut pb) => pb.post_work(),
        }
    }
}

impl Allocate for Generic {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) {
        self.allocate()
    }

    fn pre_work(&mut self) { self.pre_work(); }
    fn post_work(&mut self) { self.post_work(); }
}


/// Enumerations of constructable implementors of `Allocate`.
///
/// The builder variants are meant to be `Send`, so that they can be moved across threads,
/// whereas the allocator they construct may not. As an example, the `ProcessBinary` type
/// contains `Rc` wrapped state, and so cannot itself be moved across threads.
pub enum GenericBuilder {
    Thread(Thread),
    Process(Process),
    Binary(Binary),
    ProcessBinary(ProcessBinaryBuilder<::allocator::process_binary::vec::VecBytesExchange>),
}

impl GenericBuilder {
    pub fn build(self) -> Generic {
        match self {
            GenericBuilder::Thread(t) => Generic::Thread(t),
            GenericBuilder::Process(p) => Generic::Process(p),
            GenericBuilder::Binary(b) => Generic::Binary(b),
            GenericBuilder::ProcessBinary(pb) => Generic::ProcessBinary(pb.build()),
        }
    }
}
