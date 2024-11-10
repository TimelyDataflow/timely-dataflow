//! Types wrapping typed data.

use bytes::arc::Bytes;
use crate::Data;

/// A wrapped message which supports serialization and deserialization.
pub struct Message<T> {
    /// Message contents.
    pub payload: T,
}

impl<T> Message<T> {
    /// Wrap a typed item as a message.
    pub fn from_typed(typed: T) -> Self {
        Message { payload: typed }
    }
}

impl<T: Data> Message<T> {
    /// Wrap bytes as a message.
    pub fn from_bytes(bytes: Bytes) -> Self {
        let typed = ::bincode::deserialize(&bytes[..]).expect("bincode::deserialize() failed");
        Message { payload: typed }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        ::bincode::serialized_size(&self.payload).expect("bincode::serialized_size() failed") as usize
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        ::bincode::serialize_into(writer, &self.payload).expect("bincode::serialize_into() failed");
    }
}

impl<T> ::std::ops::Deref for Message<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}
