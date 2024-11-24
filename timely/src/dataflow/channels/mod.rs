//! Structured communication between timely dataflow operators.

use serde::{Deserialize, Serialize};
use crate::communication::Push;
use crate::Container;

/// A collection of types that may be pushed at.
pub mod pushers;
/// A collection of types that may be pulled from.
pub mod pullers;
/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;

/// A serializable representation of timestamped data.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message<T, C> {
    /// The timestamp associated with the message.
    pub time: T,
    /// The data in the message.
    pub data: C,
    /// The source worker.
    pub from: usize,
    /// A sequence number for this worker-to-worker stream.
    pub seq: usize,
}

impl<T, C> Message<T, C> {
    /// Default buffer size.
    #[deprecated = "Use timely::buffer::default_capacity instead"]
    pub fn default_length() -> usize {
        crate::container::buffer::default_capacity::<C>()
    }
}

impl<T, C: Container> Message<T, C> {
    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: C, from: usize, seq: usize) -> Self {
        Message { time, data, from, seq }
    }

    /// Forms a message, and pushes contents at `pusher`. Replaces `buffer` with what the pusher
    /// leaves in place, or the container's default element. The buffer is cleared.
    #[inline]
    pub fn push_at<P: Push<Message<T, C>>>(buffer: &mut C, time: T, pusher: &mut P) {

        let data = ::std::mem::take(buffer);
        let message = Message::new(time, data, 0, 0);
        let mut bundle = Some(message);

        pusher.push(&mut bundle);

        if let Some(message) = bundle {
            *buffer = message.data;
            buffer.clear();
        }
    }
}

// Instructions for serialization of `Message`.
// Intended to swap out the constraint on `C` for `C: Bytesable`.
impl<T, C> crate::communication::Bytesable for Message<T, C>
where
    T: Serialize + for<'a> Deserialize<'a>,
    C: Serialize + for<'a> Deserialize<'a>,
{
    fn from_bytes(bytes: crate::bytes::arc::Bytes) -> Self {
        ::bincode::deserialize(&bytes[..]).expect("bincode::deserialize() failed")
    }

    fn length_in_bytes(&self) -> usize {
        ::bincode::serialized_size(&self).expect("bincode::serialized_size() failed") as usize
    }

    fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        ::bincode::serialize_into(writer, &self).expect("bincode::serialize_into() failed");
    }
}