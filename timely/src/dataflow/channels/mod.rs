//! Structured communication between timely dataflow operators.

use crate::{Container, ContainerBuilder};
use crate::communication::Push;
use crate::communication::message::FromAllocated;

/// A collection of types that may be pushed at.
pub mod pushers;
/// A collection of types that may be pulled from.
pub mod pullers;
/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;

/// The input to and output from timely dataflow communication channels.
pub type BundleCore<T, D> = crate::communication::Message<Message<T, D>>;

/// The input to and output from timely dataflow communication channels specialized to vectors.
pub type Bundle<T, D> = BundleCore<T, Vec<D>>;

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
    pub fn push_at<P: Push<BundleCore<T, D>, MessageAllocation<D::Allocation>>>(data: Option<D>, time: T, pusher: &mut P, allocation: &mut Option<D::Allocation>)
    where D: Container
    {
        if let Some(data) = data {
            let message = Message::new(time, data, 0, 0);
            let mut bundle = Some(BundleCore::from_typed(message));
            let mut bundle_allocation = None;
            pusher.push(bundle, &mut bundle_allocation);

            if let Some(message) = bundle_allocation {
                *allocation = Some(message);
            }

            // TODO: Unclear we always want this here.
            // if buffer.capacity() != D::default_length() {
            //     *buffer = D::Builder::with_capacity(D::default_length()).build();
            // }
        }
    }
}

pub struct MessageAllocation<C> {
    pub data: Option<C>,
}

impl<T, C: Container+FromAllocated<C::Allocation>> From<Message<T, C>> for MessageAllocation<C::Allocation> {
    fn from(message: Message<T, C>) -> Self {
        MessageAllocation { data: message.data.hollow() }
    }
}