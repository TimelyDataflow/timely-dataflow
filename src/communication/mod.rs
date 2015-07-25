use std::fmt::Debug;
use std::any::Any;

pub use communication::allocator::ThreadCommunicator;
pub use communication::allocator::ProcessCommunicator;
pub use communication::allocator::BinaryCommunicator;
pub use communication::pact::ParallelizationContract;
pub use communication::allocator::{Communicator};
pub use communication::pushpull::{Pushable, Pullable};

pub mod allocator;
pub mod pact;
pub mod pushpull;

pub trait Data : Clone+Send+Debug+Any { }
impl<T: Clone+Send+Debug+Any> Data for T { }
