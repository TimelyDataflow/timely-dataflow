//! Methods and structures for communication between timely dataflow components.

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

/// Composite trait for types that may be used as data payloads in timely dataflow.
///
/// Several of the required traits are for convenience rather than necessity (e.g. `Debug`), but it
/// is worth understanding which are truly mandatory, and which could be relaxed in some contexts.
pub trait Data : Clone+Send+Debug+Any+Serializable { }
impl<T: Clone+Send+Debug+Any+Serializable> Data for T { }
