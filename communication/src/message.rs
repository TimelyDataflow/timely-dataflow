//! Types wrapping typed data.

use bytes::arc::Bytes;
use crate::Data;

/// A wrapped message which may be either typed or binary data.
pub struct Message<T> {
    payload: T,
}

impl<T> Message<T> {
    /// Wrap a typed item as a message.
    pub fn from_typed(typed: T) -> Self {
        Message { payload: typed }
    }
    /// Destructures and returns any typed data.
    pub fn if_typed(self) -> Option<T> {
        Some(self.payload)
    }
    /// Returns a mutable reference, if typed.
    pub fn if_mut(&mut self) -> Option<&mut T> {
        Some(&mut self.payload)
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

impl<T: Clone> Message<T> {
    /// Produces a typed instance of the wrapped element.
    pub fn into_typed(self) -> T {
        self.payload
    }
    /// Ensures the message is typed data and returns a mutable reference to it.
    pub fn as_mut(&mut self) -> &mut T {
        &mut self.payload
    }
}