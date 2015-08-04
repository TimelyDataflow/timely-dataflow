pub use self::thread::Thread;
pub use self::process::Process;
pub use self::binary::Binary;
pub use self::generic::Generic;

pub mod thread;
pub mod process;
pub mod binary;
pub mod generic;

use std::any::Any;
use super::Serialize;

/// Describes types that may be sent along a Fabric. Specific
pub trait Data : Send+Any+Serialize+Clone+'static { }
impl<T: Clone+Send+Any+Serialize+'static> Data for T { }



// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.
pub trait Allocate: 'static {
    fn index(&self) -> usize;     // number out of peers
    fn peers(&self) -> usize;     // number of peers
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>);
}

pub trait Push<T> {
    /// Provides the opportunity to take the element. A non-`None` value of `element` only indicates
    /// that the recipient did not need to consume the element, not that it was not pushed. This can
    /// be useful when `T` contains owned resources that can be re-used.
    fn push(&mut self, element: &mut Option<T>);
    fn give(&mut self, element: T) { self.push(&mut Some(element)); }
    fn done(&mut self) { self.push(&mut None); }
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    fn push(&mut self, element: &mut Option<T>) { (**self).push(element) }
}


pub trait Pull<T> {
    /// Provides the opportunity to take the element. The receiver is free to `.take()` the option,
    /// and the implementor must not expect otherwise.
    fn pull(&mut self) -> &mut Option<T>;
    fn take(&mut self) -> Option<T> { self.pull().take() }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    fn pull(&mut self) -> &mut Option<T> { (**self).pull() }
}
