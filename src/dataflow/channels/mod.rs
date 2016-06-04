//! Structured communication between timely dataflow operators.

pub use self::message::Message;
pub use self::message::Content;

/// A collection of types that may be pushed at.
pub mod pushers;
/// A collection of types that may be pulled from.
pub mod pullers;
/// Types relating to batching of timestamped records.
pub mod message;
/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;
