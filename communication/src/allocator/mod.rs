//! Types and traits for the allocation of channels.

pub use self::thread::Thread;
pub use self::process::Process;
pub use self::generic::{Generic, GenericBuilder};

pub mod thread;
pub mod process;
pub mod generic;

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
    /// Work performed before scheduling dataflows.
    fn pre_work(&mut self) { }
    /// Work performed after scheduling dataflows.
    fn post_work(&mut self) { }
}
