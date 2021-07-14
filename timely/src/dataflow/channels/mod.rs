//! Structured communication between timely dataflow operators.

use crate::communication::Push;

/// A collection of types that may be pushed at.
pub mod pushers;
/// A collection of types that may be pulled from.
pub mod pullers;
/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;

/// The input to and output from timely dataflow communication channels.
pub type Bundle<T, D> = crate::communication::Message<Message<T, D>>;

/// A serializable representation of timestamped data.
#[derive(Clone, Abomonation, Serialize, Deserialize)]
pub struct Message<T, D> {
    /// The timestamp associated with the message.
    pub time: T,
    /// The data in the message.
    pub data: Vec<D>,
    /// The source worker.
    pub from: usize,
    /// A sequence number for this worker-to-worker stream.
    pub seq: usize,
}

impl<T, D> Message<T, D> {
    /// Default buffer size.
    pub fn default_length() -> usize {
        1024
    }

    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: Vec<D>, from: usize, seq: usize) -> Self {
        Message { time, data, from, seq }
    }

    /// Forms a message, and pushes contents at `pusher`. Replaces `buffer` with what the pusher
    /// leaves in place, or a new empty `Vec`.
    #[inline]
    pub fn push_at<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {

        let data = ::std::mem::replace(buffer, Vec::new());
        let message = Message::new(time, data, 0, 0);
        let mut bundle = Some(Bundle::from_typed(message));

        pusher.push(&mut bundle);

        if let Some(message) = bundle {
            if let Some(message) = message.if_typed() {
                *buffer = message.data;
                buffer.clear();
            }
        }

        // Avoid memory leaks by buffers growing out of bounds
        if buffer.capacity() > Self::default_length() {
            *buffer = Vec::with_capacity(Self::default_length());
        }
    }
}
