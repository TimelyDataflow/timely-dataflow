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
#[derive(Clone)]
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
//
// Serialization of each field is meant to be `u64` aligned, so that each has tha ability
// to be decoded using safe transmutation, e.g. `bytemuck`.
impl<T, C> crate::communication::Bytesable for Message<T, C>
where
    T: Serialize + for<'a> Deserialize<'a>,
    C: ContainerBytes,
{
    fn from_bytes(mut bytes: crate::bytes::arc::Bytes) -> Self {
        use byteorder::ReadBytesExt;
        let mut slice = &bytes[..];
        let from: usize = slice.read_u64::<byteorder::LittleEndian>().unwrap().try_into().unwrap();
        let seq: usize = slice.read_u64::<byteorder::LittleEndian>().unwrap().try_into().unwrap();
        let time: T = ::bincode::deserialize_from(&mut slice).expect("bincode::deserialize() failed");
        let time_size = ::bincode::serialized_size(&time).expect("bincode::serialized_size() failed") as usize;
        // We expect to find the `data` payload at `8 + 8 + round_up(time_size)`;
        let bytes_read = 8 + 8 + ((time_size + 7) & !7);
        // let bytes_read = bytes.len() - slice.len();
        bytes.extract_to(bytes_read);
        let data: C = ContainerBytes::from_bytes(bytes);
        Self { time, data, from, seq }
    }

    fn length_in_bytes(&self) -> usize {
        let time_size = ::bincode::serialized_size(&self.time).expect("bincode::serialized_size() failed") as usize;
        // 16 comes from the two `u64` fields: `from` and `seq`.
        16 + ((time_size + 7) & !7) + self.data.length_in_bytes()
    }

    fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        use byteorder::WriteBytesExt;
        writer.write_u64::<byteorder::LittleEndian>(self.from.try_into().unwrap()).unwrap();
        writer.write_u64::<byteorder::LittleEndian>(self.seq.try_into().unwrap()).unwrap();
        ::bincode::serialize_into(&mut *writer, &self.time).expect("bincode::serialize_into() failed");
        let time_size = ::bincode::serialized_size(&self.time).expect("bincode::serialized_size() failed") as usize;
        let time_slop = ((time_size + 7) & !7) - time_size;
        writer.write(&[0u8; 8][..time_slop]).unwrap();
        self.data.into_bytes(&mut *writer);
    }
}


/// A container-oriented version of `Bytesable` that can be implemented here for `Vec<T>` and other containers.
pub trait ContainerBytes {
    /// Wrap bytes as `Self`.
    fn from_bytes(bytes: crate::bytes::arc::Bytes) -> Self;

    /// The number of bytes required to serialize the data.
    fn length_in_bytes(&self) -> usize;

    /// Writes the binary representation into `writer`.
    fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W);
}

mod implementations {

    use serde::{Serialize, Deserialize};
    use crate::dataflow::channels::ContainerBytes;

    impl<T: Serialize + for<'a> Deserialize<'a>> ContainerBytes for Vec<T> {
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

    use crate::container::flatcontainer::FlatStack;
    impl<T: Serialize + for<'a> Deserialize<'a> + crate::container::flatcontainer::Region> ContainerBytes for FlatStack<T> {
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
}
