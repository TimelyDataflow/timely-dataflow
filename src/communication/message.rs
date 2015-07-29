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

enum Contents<D> {
    Bytes(Vec<u8>, usize, usize),  // bytes[offset..] decodes to length elements
    Typed(Vec<D>),
}

/// ALLOC : This Drop implementation gets *very* angry if we drop allocated data.
/// ALLOC : It probably shouldn't be used in practice, but should help track down who is being
/// ALLOC : bad about respecting allocated memory.
// impl<D> Drop for Message<D> {
//     match self.contents {
//         Contents::Bytes(bytes, _, _) => { assert!(bytes.capacity() == 0); }
//         Contents::Typed(typed) => { assert!(typed.capacity() == 0); }
//     }
// }

impl<D> Message<D> {
    /// Default number of elements in a typed allocated message. This could vary as a function of
    /// `std::mem::size_of::<D>()`, so is left as a method rather than a constant.
    pub fn default_length() -> usize { 4096 }

    /// Constructs a `Message` from typed data, replacing its argument with `Vec::new()`.
    pub fn from_typed(typed: &mut Vec<D>) -> Message<D> {
        Message { contents: Contents::Typed(::std::mem::replace(typed, Vec::new())) }
    }
    /// The length of the underlying typed vector.
    ///
    /// The length is tracked without needing to deserialize the data, so that this method can be
    /// called even for `D` that do not implement `Serializable`.
    pub fn len(&self) -> usize {
        match self.contents {
            Contents::Bytes(_, _, length) => length,
            Contents::Typed(ref data) => data.len(),
        }
    }
    /// Returns the typed vector, cleared, or a Vec::new() if the data are binary (and drops them
    /// on the floor, I guess! Ouch.
    /// ALLOC : dropping of binary data. likely called only by persons who pushed typed data on,
    /// ALLOC : so perhaps not all that common. Could put a panic! here just for fun! :D
    pub fn into_typed(self) -> Vec<D> {
        match self.contents {
            Contents::Bytes(_,_,_) => Vec::new(),
            Contents::Typed(mut data) => { data.clear(); data },
        }
    }

    // pub fn test_capacity(&self) {
    //     if let Contents::Typed(ref data) = self.contents {
    //         assert!(data.capacity() == 0 || data.capacity() == Message::<D>::default_length());
    //     }
    // }
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

// TODO : Rather than .clone() the decoded data, we should try and re-rig serialization so that the
// TODO : underlying byte array can just be handed to Vec::from_raw_parts, cloning any owned data.
// TODO : I think we would need to make sure that the byte array had the right alignment, so that
// TODO : when the Vec is eventually dropped we don't de-allocate the wrong number of bytes.
// TODO : This require mucking with the Abomonation code, as it doesn't currently let you step in
// TODO : and skip copying the 24 byte Vec struct first. We'd also have to bake in the typed length
// TODO : somewhere outside of this serialized hunk of data.
impl<D: Clone+Serializable> DerefMut for Message<D> {
    fn deref_mut(&mut self) -> &mut Vec<D> {
        let value = if let Contents::Bytes(ref mut bytes, offset, _length) = self.contents {
            let (data, _) = <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap();
            // ALLOC : clone() will allocate a Vec<D> and maybe more.
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
