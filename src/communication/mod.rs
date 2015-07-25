pub use self::communicator::Communicator;
pub use self::observer::Observer;
pub use self::pullable::Pullable;
pub use self::message::Message;

use std::any::Any;
use std::fmt::Debug;

use serialization::Serializable;

pub mod communicator;
pub mod observer;
pub mod pullable;
pub mod message;
pub mod pact;

pub trait Data : Clone+Send+Debug+Any+Serializable { }
impl<T: Clone+Send+Debug+Any+Serializable> Data for T { }
