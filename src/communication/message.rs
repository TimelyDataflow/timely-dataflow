//! Core type for communicated a collection of `D: Data` records.
//!
//! `Message<D>` is meant to be treated as a `Vec<D>`, with the caveat that it may wrap either
//! typed `Vec<D>` data or binary `Vec<u8>` data that have not yet been deserialized. The type
//! implements `Deref` and `DerefMut` with `Target = Vec<D>`, whose implementations accommodate
//! the possibly serialized representation.
use serialization::Serializable;
use std::ops::{Deref, DerefMut};

pub struct Message<D> {
    contents: Contents<D>,
}

impl<D> Message<D> {
    /// Constructs a `Message` from typed data, replacing its argument with `Vec::new()`.
    pub fn from_typed(typed: &mut Vec<D>) -> Message<D> {
        Message { contents: Contents::Typed(::std::mem::replace(typed, Vec::new())) }
    }
    /// The length of the underlying vector.
    ///
    /// The length is tracked without needing to deserialize the data, so that this method can be
    /// called even for `D` that do not implement `Serializable`.
    pub fn len(&self) -> usize {
        match self.contents {
            Contents::Bytes(_, _, length) => length,
            Contents::Typed(ref data) => data.len(),
        }
    }
    /// Returns the typed vector, cleared, or allocates a new vector of supplied capacity if the
    /// `Message` is backed by binary data.
    pub fn into_typed(self, capacity: usize) -> Vec<D> {
        match self.contents {
            Contents::Bytes(_,_,_) => Vec::with_capacity(capacity),
            Contents::Typed(mut data) => { data.clear(); data },
        }
    }
}

impl<D: Serializable> Message<D> {
    /// Constructs a `Message` from binary data, starting at `offset` in `bytes`.
    ///
    /// This method calls `decode` on the binary data, ensuring that pointers are properly
    /// adjusted, and that future calls to `verify` should succeed.
    pub fn from_bytes(mut bytes: Vec<u8>, offset: usize) -> Message<D> {
        let length = <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap().0.len();
        Message { contents: Contents::Bytes(bytes, offset, length) }
    }
}

impl<D: Serializable> Deref for Message<D> {
    type Target = Vec<D>;
    fn deref(&self) -> &Vec<D> {
        match self.contents {
            // NOTE : decoded in Contents::Byte constructor. verification should pass.
            // NOTE : privacy of Contents should mean cannot construct w/o validation.
            Contents::Bytes(ref bytes, offset, _length) => {
                <Vec<D> as Serializable>::verify(&bytes[offset..]).unwrap().0
            },
            Contents::Typed(ref data) => data,
        }
    }
}

impl<D: Clone+Serializable> DerefMut for Message<D> {
    fn deref_mut(&mut self) -> &mut Vec<D> {
        let value = if let Contents::Bytes(ref mut bytes, offset, _length) = self.contents {
            let (data, _) = <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap();
            Some(Contents::Typed((*data).clone()))
        }
        else { None };

        if let Some(contents) = value {
            self.contents = contents;
        }

        if let Contents::Typed(ref mut data) = self.contents {
            data
        }
        else { unreachable!() }
    }
}

enum Contents<D> {
    Bytes(Vec<u8>, usize, usize),  // bytes[offset..] decodes to length elements
    Typed(Vec<D>),
}

// impl<D> Message<D> {
//     pub fn len(&self) -> usize {
//         match *self {
//             Message::Bytes(_, _, length) => length,
//             Message::Typed(ref data) => data.len(),
//         }
//     }
// }
// impl<D: Serializable> Message<D> {
//     pub fn look(&mut self) -> &Vec<D> {
//         match *self {
//             Message::Bytes(ref mut bytes, offset, _length) => {
//                 <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap().0
//             },
//             Message::Typed(ref data) => data,
//         }
//     }
// }
// impl<D: Clone+Serializable> Message<D> {
//     pub fn take(&mut self) -> &mut Vec<D> {
//         let value = if let Message::Bytes(ref mut bytes, offset, _length) = *self {
//             let (data, _) = <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap();
//             Some(Message::Typed((*data).clone()))
//         }
//         else { None };
//
//         if let Some(message) = value {
//             *self = message;
//         }
//
//         if let Message::Typed(ref mut data) = *self {
//             data
//         }
//         else { panic!{} }
//     }
// }
