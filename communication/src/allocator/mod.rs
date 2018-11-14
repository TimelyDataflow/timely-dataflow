//! Types and traits for the allocation of channels.

use std::rc::Rc;
use std::cell::RefCell;

pub use self::thread::Thread;
pub use self::process::Process;
pub use self::generic::{Generic, GenericBuilder};

pub mod thread;
pub mod process;
pub mod generic;

pub mod canary;
pub mod counters;

pub mod zero_copy;

use {Data, Push, Pull, Message};

/// A proto-allocator, which implements `Send` and can be completed with `build`.
///
/// This trait exists because some allocators contain non-Send elements, like `Rc` wrappers for
/// shared state. As such, what we actually need to create to initialize a computation are builders,
/// which we can then spawn in new threads each of which then construct their actual allocator.
pub trait AllocateBuilder : Send {
    /// The type of built allocator.
    type Allocator: Allocate;
    /// Builds allocator, consumes self.
    fn build(self) -> Self::Allocator;
}

/// A type capable of allocating channels.
///
/// There is some feature creep, in that this contains several convenience methods about the nature
/// of the allocated channels, and maintenance methods to ensure that they move records around.
pub trait Allocate {
    /// The index of the worker out of `(0..self.peers())`.
    fn index(&self) -> usize;
    /// The number of workers.
    fn peers(&self) -> usize;
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>);
    ///
    fn counts(&self) -> &Rc<RefCell<Vec<(usize, i64)>>>;

    /// Reports the changes in message counts in each channel.
    fn receive(&mut self, action: impl Fn(&[(usize,i64)])) {
        let mut borrow = self.counts().borrow_mut();
        action(&borrow[..]);
        borrow.clear();
    }
    /// Work performed after scheduling dataflows.
    fn flush(&mut self) { }

    /// Constructs a pipeline channel from the worker to itself.
    ///
    /// By default this method uses the native channel allocation mechanism, but the expectation is
    /// that this behavior will be overriden to be more efficient.
    fn pipeline<T: 'static>(&mut self, identifier: usize) -> (thread::ThreadPusher<Message<T>>, thread::ThreadPuller<Message<T>>) {
        thread::Thread::new_from(identifier, self.counts().clone())
    }
}
