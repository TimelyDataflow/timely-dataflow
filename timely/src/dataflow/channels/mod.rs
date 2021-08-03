//! Structured communication between timely dataflow operators.

use crate::{Container, ContainerBuilder};
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
    pub data: D,
    /// The source worker.
    pub from: usize,
    /// A sequence number for this worker-to-worker stream.
    pub seq: usize,
}

impl<T, D> Message<T, D> {
    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: D, from: usize, seq: usize) -> Self {
        Message { time, data, from, seq }
    }

    /// Forms a message, and pushes contents at `pusher`.
    #[inline]
    pub fn push_at<P: Push<Bundle<T, D>>>(buffer: &mut D, time: T, pusher: &mut P)
    where D: Container
    {

        let data = D::take(buffer);
        let message = Message::new(time, data, 0, 0);
        let mut bundle = Some(Bundle::from_typed(message));

        pusher.push(&mut bundle);

        if let Some(message) = bundle {
            if let Some(message) = message.if_typed() {
                *buffer = D::Builder::with_allocation(message.data).build();
            }
        }

        // TODO: Unclear we always want this here.
        if buffer.capacity() != D::default_length() {
            *buffer = D::Builder::with_capacity(D::default_length()).build();
        }
    }}
