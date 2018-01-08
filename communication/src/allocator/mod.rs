//! Types and traits for the allocation of channels between threads, process, and computers.

pub use self::thread::Thread;
pub use self::process::Process;
pub use self::binary::Binary;
pub use self::generic::Generic;

pub mod thread;
pub mod process;
pub mod binary;
pub mod generic;

use {Data, Push, Pull};

// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.
pub trait Allocate {
    /// The index of the worker out of `(0..self.peers())`.
    fn index(&self) -> usize;
    /// The number of workers.
    fn peers(&self) -> usize;
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>);
}
