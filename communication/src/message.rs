//! Types wrapping typed data.

use bytes::arc::Bytes;
use crate::Data;

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

impl<'a, T: 'a> ::std::borrow::Borrow<T> for RefOrMut<'a, T> {
    fn borrow(&self) -> &T {
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

    /// Extracts the contents of `self`, either by cloning, or swapping and leaving a default
    /// element in place.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn take(self) -> T where T: Default {
        let mut element = Default::default();
        self.swap(&mut element);
        element
    }
}

/// A wrapped message which may be either typed or binary data.
pub struct Message<T> {
    payload: MessageContents<T>,
}

/// Possible returned representations from a channel.
enum MessageContents<T> {
    /// Rust typed instance. Available for ownership.
    Owned(T),
}

impl<T> Message<T> {
    /// Wrap a typed item as a message.
    pub fn from_typed(typed: T) -> Self {
        Message { payload: MessageContents::Owned(typed) }
    }
    /// Destructures and returns any typed data.
    pub fn if_typed(self) -> Option<T> {
        match self.payload {
            MessageContents::Owned(typed) => Some(typed),
        }
    }
    /// Returns a mutable reference, if typed.
    pub fn if_mut(&mut self) -> Option<&mut T> {
        match &mut self.payload {
            MessageContents::Owned(typed) => Some(typed),
        }
    }
    /// Returns an immutable or mutable typed reference.
    ///
    /// This method returns a mutable reference if the underlying data are typed Rust
    /// instances, which admit mutation, and it returns an immutable reference if the
    /// data are serialized binary data.
    pub fn as_ref_or_mut(&mut self) -> RefOrMut<T> {
        match &mut self.payload {
            MessageContents::Owned(typed) => { RefOrMut::Mut(typed) },
        }
    }
}

impl<T: Data> Message<T> {
    /// Wrap bytes as a message.
    pub fn from_bytes(bytes: Bytes) -> Self {
        let typed = ::bincode::deserialize(&bytes[..]).expect("bincode::deserialize() failed");
        Message { payload: MessageContents::Owned(typed) }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        match &self.payload {
            MessageContents::Owned(typed) => {
                ::bincode::serialized_size(&typed).expect("bincode::serialized_size() failed") as usize
            },
        }
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match &self.payload {
            MessageContents::Owned(typed) => {
                ::bincode::serialize_into(writer, &typed).expect("bincode::serialize_into() failed");
            },
        }
    }
}

impl<T> ::std::ops::Deref for Message<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // TODO: In principle we have already decoded, but let's go again
        match &self.payload {
            MessageContents::Owned(typed) => { typed },
        }
    }
}

impl<T: Clone> Message<T> {
    /// Produces a typed instance of the wrapped element.
    pub fn into_typed(self) -> T {
        match self.payload {
            MessageContents::Owned(instance) => instance,
        }
    }
    /// Ensures the message is typed data and returns a mutable reference to it.
    pub fn as_mut(&mut self) -> &mut T {

        let cloned: Option<T> = match &self.payload {
            MessageContents::Owned(_) => None,
        };

        if let Some(cloned) = cloned {
            self.payload = MessageContents::Owned(cloned);
        }

        let MessageContents::Owned(typed) = &mut self.payload;
        typed
    }
}