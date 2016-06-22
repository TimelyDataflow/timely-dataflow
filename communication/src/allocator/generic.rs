//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use allocator::{Allocate, Thread, Process, Binary};
use {Push, Pull, Data};

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Generic {
    Thread(Thread),
    Process(Process),
    Binary(Binary),
}

impl Generic {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.index(),
            &Generic::Process(ref p) => p.index(),
            &Generic::Binary(ref b) => b.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.peers(),
            &Generic::Process(ref p) => p.peers(),
            &Generic::Binary(ref b) => b.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    pub fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) {
        match self {
            &mut Generic::Thread(ref mut t) => t.allocate(),
            &mut Generic::Process(ref mut p) => p.allocate(),
            &mut Generic::Binary(ref mut b) => b.allocate(),
        }
    }
}

impl Allocate for Generic {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) { 
        self.allocate()
    }
}
