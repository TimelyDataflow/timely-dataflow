//! Core type for communicating a collection of `D: Data` records.
//!
//! `Message<D>` is meant to be treated as a `Vec<D>`, with the caveat that it may wrap either
//! typed `Vec<D>` data or binary `Vec<u8>` data that have not yet been deserialized. The type
//! implements `Deref` and `DerefMut` with `Target = Vec<D>`, whose implementations accommodate
//! the possibly serialized representation.
use timely_communication::{Serialize, Push};
use std::ops::{Deref, DerefMut};
use abomonation::{Abomonation, encode, decode};

#[derive(Clone)]
pub struct Message<T, D> {
    pub time: T,
    pub data: Content<D>,
}

// Implementation required to get different behavior out of communication fabric.
impl<T: Abomonation+Clone, D: Abomonation> Serialize for Message<T, D> {
    fn into_bytes(&mut self, bytes: &mut Vec<u8>) {
        unsafe { encode(&self.time, bytes); }
        let vec: &Vec<D> = self.data.deref();
        unsafe { encode(vec, bytes); }
    }
    fn from_bytes(bytes: &mut Vec<u8>) -> Self {
        let mut bytes = ::std::mem::replace(bytes, Vec::new());
        let x_len = bytes.len();
        let (time, offset) = {
            let (t,r) = unsafe { decode::<T>(&mut bytes) }.unwrap();
            let o = x_len - r.len();
            ((*t).clone(), o)
        };

        let length = unsafe { decode::<Vec<D>>(&mut bytes[offset..]) }.unwrap().0.len();
        Message { time: time, data: Content::Bytes(bytes, offset, length) }
    }
}


#[derive(Clone)]
pub enum Content<D> {
    Bytes(Vec<u8>, usize, usize),  // bytes[offset..] decodes to length elements
    Typed(Vec<D>),
}

// ALLOC : This Drop implementation gets *very* angry if we drop allocated data.
// ALLOC : It probably shouldn't be used in practice, but should help track down who is being
// ALLOC : bad about respecting allocated memory.
// impl<D> Drop for Message<D> {
//     match self.contents {
//         Content::Bytes(bytes, _, _) => { assert!(bytes.capacity() == 0); }
//         Content::Typed(typed) => { assert!(typed.capacity() == 0); }
//     }
// }

impl<D> Content<D> {
    /// Default number of elements in a typed allocated message. This could vary as a function of
    /// `std::mem::size_of::<D>()`, so is left as a method rather than a constant.
    pub fn default_length() -> usize { 4096 }

    /// The length of the underlying typed vector.
    ///
    /// The length is tracked without needing to deserialize the data, so that this method can be
    /// called even for `D` that do not implement `Serializable`.
    pub fn len(&self) -> usize {
        match *self {
            Content::Bytes(_, _, length) => length,
            Content::Typed(ref data) => data.len(),
        }
    }


    /// Constructs a `Message` from typed data, replacing its argument with `Vec::new()`.
    pub fn from_typed(typed: &mut Vec<D>) -> Content<D> {
        Content::Typed(::std::mem::replace(typed, Vec::new()))
    }

    /// Returns the typed vector, cleared, or a Vec::new() if the data are binary (and drops them
    /// on the floor, I guess! Ouch.
    /// ALLOC : dropping of binary data. likely called only by persons who pushed typed data on,
    /// ALLOC : so perhaps not all that common. Could put a panic! here just for fun! :D
    /// ALLOC : casual dropping of contents of `data`, which might have allocated memory.
    pub fn into_typed(self) -> Vec<D> {
        match self {
            Content::Bytes(_,_,_) => Vec::new(),
            Content::Typed(mut data) => { data.clear(); data },
        }
    }

    pub fn push_at<T, P: Push<(T, Content<D>)>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {

        let data = Content::from_typed(buffer);
        let mut message = Some((time, data));

        pusher.push(&mut message);

        if let Some((_, Content::Typed(mut typed))) = message {
            typed.clear();
            *buffer = typed;
        }
        else {
            *buffer = Vec::with_capacity(Content::<D>::default_length());
        }

        assert!(buffer.capacity() == Content::<D>::default_length());
    }
}


impl<D: Abomonation> Deref for Content<D> {
    type Target = Vec<D>;
    fn deref(&self) -> &Vec<D> {
        match *self {
            Content::Bytes(ref bytes, offset, _length) => {
                // verify wasn't actually safe, it turns out...
                unsafe { ::std::mem::transmute(bytes.get_unchecked(offset)) }
            },
            Content::Typed(ref data) => data,
        }
    }
}

// TODO : Rather than .clone() the decoded data, we should try and re-rig serialization so that the
// TODO : underlying byte array can just be handed to Vec::from_raw_parts, cloning any owned data.
// TODO : I think we would need to make sure that the byte array had the right alignment, so that
// TODO : when the Vec is eventually dropped we don't de-allocate the wrong number of bytes.
// TODO : This requires mucking with the Abomonation code, as it doesn't currently let you step in
// TODO : and skip copying the 24 byte Vec struct first. We'd also have to bake in the typed length
// TODO : somewhere outside of this serialized hunk of data.
impl<D: Clone+Abomonation> DerefMut for Content<D> {
    fn deref_mut(&mut self) -> &mut Vec<D> {
        let value = if let Content::Bytes(ref mut bytes, offset, _length) = *self {
            let data: &Vec<D> = unsafe { ::std::mem::transmute(bytes.get_unchecked(offset)) };
            // let (data, _) = verify::<Vec<D>>(&bytes[offset..]).unwrap();
            // ALLOC : clone() will allocate a Vec<D> and maybe more.
            Some(Content::Typed((*data).clone()))
        }
        else { None };

        if let Some(contents) = value {
            *self = contents;
        }

        if let Content::Typed(ref mut data) = *self {
            data
        }
        else { unreachable!() }
    }
}
