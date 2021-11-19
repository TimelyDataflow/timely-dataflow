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
        const MESSAGE_BUFFER_SIZE: usize = 1 << 13;
        let size = std::mem::size_of::<D>();
        if size == 0 {
            // We could use usize::MAX here, but to avoid overflows we
            // limit the default length for zero-byte types.
            MESSAGE_BUFFER_SIZE
        } else if size <= MESSAGE_BUFFER_SIZE {
            MESSAGE_BUFFER_SIZE / size
        } else {
            1
        }
    }

    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: Vec<D>, from: usize, seq: usize) -> Self {
        Message { time, data, from, seq }
    }

    /// Forms a message, and pushes contents at `pusher`. Replaces `buffer` with what the pusher
    /// leaves in place, or a new `Vec`. Note that the returned vector is always initialized with
    /// a capacity of [Self::default_length] elements.
    #[inline]
    pub fn push_at<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {

        Self::push_at_no_allocation(buffer, time, pusher);

        // TODO: Unclear we always want this here.
        if buffer.capacity() != Self::default_length() {
            *buffer = Vec::with_capacity(Self::default_length());
        }
    }

    /// Forms a message, and pushes contents at `pusher`. Replaces `buffer` with what the pusher
    /// leaves in place, or a new empty `Vec`. If the pusher leaves a vector with a capacity larger
    /// than [Self::default_length], the vector is initialized with a new vector with
    /// [Self::default_length] capacity.
    #[inline]
    pub fn push_at_no_allocation<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {

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
