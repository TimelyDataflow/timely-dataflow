//! Structured communication between timely dataflow operators.

use crate::communication::{Push, Container, IntoAllocated};
use crate::communication::message::{MessageAllocation};
use crate::communication::message::RefOrMut;

/// A collection of types that may be pushed at.
pub mod pushers;
/// A collection of types that may be pulled from.
pub mod pullers;
/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;

/// The input to and output from timely dataflow communication channels.
pub type BundleCore<T, D> = crate::communication::Message<Message<T, D>>;

/// Allocation for [BundleCore].
pub type BundleCoreAllocation<T, D> = <BundleCore<T, D> as Container>::Allocation;

/// The input to and output from timely dataflow communication channels specialized to vectors.
pub type Bundle<T, D> = BundleCore<T, Vec<D>>;

/// A serializable representation of timestamped data.
#[derive(Clone, Abomonation, Serialize, Deserialize)]
pub struct Message<T, D: Container> {
    /// The timestamp associated with the message.
    pub time: T,
    /// The data in the message.
    pub data: D,
    /// The source worker.
    pub from: usize,
    /// A sequence number for this worker-to-worker stream.
    pub seq: usize,
}

impl<T, D: Container> Message<T, D> {
    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: D, from: usize, seq: usize) -> Self {
        Message { time, data, from, seq }
    }

    /// Forms a message, and pushes contents at `pusher`.
    #[inline]
    pub fn push_at<P: Push<BundleCore<T, D>>>(data: Option<D>, time: T, pusher: &mut P, allocation: &mut Option<D::Allocation>)
    {
        if let Some(data) = data {
            let message = Message::new(time, data, 0, 0);
            let bundle = Some(BundleCore::from_typed(message));
            let mut bundle_allocation = None;
            pusher.push(bundle, &mut bundle_allocation);

            if let Some(MessageAllocation(Some(MessageAllocation(data)))) = bundle_allocation {
                *allocation = Some(data);
            }

            // TODO: Unclear we always want this here.
            // if buffer.capacity() != D::default_length() {
            //     *buffer = D::Builder::with_capacity(D::default_length()).build();
            // }
        }
    }
}

impl<T, D: Container> Container for Message<T, D> {
    type Allocation = MessageAllocation<D::Allocation>;

    fn hollow(self) -> Self::Allocation {
        MessageAllocation(self.data.hollow())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T, D: Container> IntoAllocated<Message<T, D>> for MessageAllocation<D::Allocation> {
    fn assemble(self, allocated: RefOrMut<Message<T, D>>) -> Message<T, D> where Self: Sized {
        todo!()
    }

    fn assemble_new(allocated: RefOrMut<Message<T, D>>) -> Message<T, D> {
        todo!()
    }
}

// impl<T, C: Container+FromAllocated<C::Allocation>> From<Message<T, C>> for MessageAllocation<C::Allocation> {
//     fn from(message: Message<T, C>) -> Self {
//         MessageAllocation { data: message.data.hollow() }
//     }
// }