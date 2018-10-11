//! Types wrapping typed data.

use bytes::arc::Bytes;
use abomonation::{Abomonation, abomonated::Abomonated, encode, measure};

/// Possible returned representations from a channel.
enum TypedOrBinary<T> {
    /// Binary representation.
    Binary(Abomonated<T, Bytes>),
    /// Rust typed instance.
    Typed(T),
}

/// Either an immutable or mutable reference.
pub enum RefOrMut<'a, T> where T: 'a {
    /// An immutable reference.
    Ref(&'a T),
    /// A mutable reference.
    Mut(&'a mut T),
}

impl<'a, T: 'a> ::std::ops::Deref for RefOrMut<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self {
            RefOrMut::Ref(reference) => reference,
            RefOrMut::Mut(reference) => reference,
        }
    }
}

impl<'a, T: Clone+'a> RefOrMut<'a, T> {
    /// Extracts the contents of `self`, either by cloning or swapping.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn swap<'b>(self, element: &'b mut T) {
        match self {
            RefOrMut::Ref(reference) => element.clone_from(reference),
            RefOrMut::Mut(reference) => ::std::mem::swap(reference, element),
        };
    }
    /// Extracts the contents of `self`, either by cloning or swapping.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn replace(self, mut element: T) -> T {
        self.swap(&mut element);
        element
    }
}

/// A wrapped message which may be either typed or binary data.
pub struct Message<T> {
    payload: TypedOrBinary<T>,
}

impl<T> Message<T> {
    /// Wrap a typed item as a message.
    pub fn from_typed(typed: T) -> Self {
        Message { payload: TypedOrBinary::Typed(typed) }
    }
    /// Destructures and returns any typed data.
    pub fn if_typed(self) -> Option<T> {
        match self.payload {
            TypedOrBinary::Binary(_) => None,
            TypedOrBinary::Typed(typed) => Some(typed),
        }
    }
    /// Returns a mutable reference, if typed.
    pub fn if_mut(&mut self) -> Option<&mut T> {
        match &mut self.payload {
            TypedOrBinary::Binary(_) => None,
            TypedOrBinary::Typed(typed) => Some(typed),
        }
    }
    /// Returns an immutable or mutable typed reference.
    ///
    /// This method returns a mutable reference if the underlying data are typed Rust
    /// instances, which admit mutation, and it returns an immutable reference if the
    /// data are serialized binary data.
    pub fn as_ref_or_mut(&mut self) -> RefOrMut<T> {
        match &mut self.payload {
            TypedOrBinary::Binary(bytes) => { RefOrMut::Ref(bytes) },
            TypedOrBinary::Typed(typed) => { RefOrMut::Mut(typed) },
        }
    }
}

/// These methods require `T` to implement `Abomonation`, for serialization functionality.
impl<T: Abomonation> Message<T> {
    /// Wrap bytes as a message.
    ///
    /// # Safety
    ///
    /// This method is unsafe, in that `Abomonated::new()` is unsafe: it presumes that
    /// the binary data can be safely decoded, which is unsafe for e.g. UTF8 data and
    /// enumerations (perhaps among many other types).
    pub unsafe fn from_bytes(bytes: Bytes) -> Self {
        let abomonated = Abomonated::new(bytes).expect("Abomonated::new() failed.");
        Message { payload: TypedOrBinary::Binary(abomonated) }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        match &self.payload {
            TypedOrBinary::Binary(bytes) => { bytes.as_bytes().len() },
            TypedOrBinary::Typed(typed) => { measure(typed) },
        }
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match &self.payload {
            TypedOrBinary::Binary(bytes) => {
                writer.write_all(bytes.as_bytes()).expect("Message::into_bytes(): write_all failed.");
            },
            TypedOrBinary::Typed(typed) => {
                unsafe { encode(typed, writer).expect("Message::into_bytes(): Abomonation::encode failed"); }
            },
        }
    }
}

impl<T> ::std::ops::Deref for Message<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // TODO: In principle we have aready decoded, but let's go again
        match &self.payload {
            TypedOrBinary::Binary(bytes) => { bytes },
            TypedOrBinary::Typed(typed) => { typed },
        }
    }
}

impl<T: Clone> Message<T> {
    /// Produces a typed instance of the wrapped element.
    pub fn into_typed(self) -> T {
        match self.payload {
            TypedOrBinary::Binary(bytes) => bytes.clone(),
            TypedOrBinary::Typed(instance) => instance,
        }
    }
    /// Ensures the message is typed data and returns a mutable reference to it.
    pub fn as_mut(&mut self) -> &mut T {
        let mut decoded = None;
        if let TypedOrBinary::Binary(bytes) = &mut self.payload {
            decoded = Some(bytes.clone());
        }
        if let Some(decoded) = decoded {
            self.payload = TypedOrBinary::Typed(decoded);
        }
        if let TypedOrBinary::Typed(typed) = &mut self.payload {
            typed
        }
        else {
            unreachable!()
        }
    }
}