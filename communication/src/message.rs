//! Types wrapping typed data.

use std::sync::Arc;
use bytes::arc::Bytes;
use abomonation;
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

mod inner {
    use crate::message::RefOrMut;

    macro_rules! ref_or_mut_inner {
        ( $($name:ident)+) => (
            #[allow(non_snake_case)]
            impl<'a, $($name,)*> RefOrMut<'a, ($($name,)*)> {
                /// Destructure the referenced tuple into references of its components.
                pub fn inner_ref(&'a self) -> ($(RefOrMut<'a, $name>,)*) {
                    match self {
                        RefOrMut::Ref(($($name,)*)) => ($(RefOrMut::Ref($name),)*),
                        RefOrMut::Mut(($($name,)*)) => ($(RefOrMut::Ref($name),)*),
                    }
                }

                /// Destructure the referenced tuple into possibly mutable references of its components.
                pub fn inner_mut(&'a mut self) -> ($(RefOrMut<'a, $name>,)*) {
                    match self {
                        RefOrMut::Ref(($(ref $name,)*)) => ($(RefOrMut::Ref($name),)*),
                        RefOrMut::Mut(($(ref mut $name,)*)) => ($(RefOrMut::Mut($name),)*),
                    }
                }
            }
        )
    }

    ref_or_mut_inner!(A);
    ref_or_mut_inner!(A B);
    ref_or_mut_inner!(A B C);
    ref_or_mut_inner!(A B C D);
    ref_or_mut_inner!(A B C D E);
    ref_or_mut_inner!(A B C D E F);
    ref_or_mut_inner!(A B C D E F G);
    ref_or_mut_inner!(A B C D E F G H);
    ref_or_mut_inner!(A B C D E F G H I);
    ref_or_mut_inner!(A B C D E F G H I J);
    ref_or_mut_inner!(A B C D E F G H I J K);
    ref_or_mut_inner!(A B C D E F G H I J K L);
    ref_or_mut_inner!(A B C D E F G H I J K L M);
    ref_or_mut_inner!(A B C D E F G H I J K L M N);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE);
    ref_or_mut_inner!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE AF);
}


/// A wrapped message which may be either typed or binary data.
pub struct Message<T> {
    payload: MessageContents<T>,
}

/// Possible returned representations from a channel.
enum MessageContents<T> {
    /// Binary representation. Only available as a reference.
    Binary(abomonation::abomonated::Abomonated<T, Bytes>),
    /// Rust typed instance. Available for ownership.
    Owned(T),
    /// Atomic reference counted. Only available as a reference.
    Arc(Arc<T>),
}

impl<T> Message<T> {
    /// Wrap a typed item as a message.
    pub fn from_typed(typed: T) -> Self {
        Message { payload: MessageContents::Owned(typed) }
    }
    /// Wrap a shared typed item as a message.
    pub fn from_arc(typed: Arc<T>) -> Self {
        Message { payload: MessageContents::Arc(typed) }
    }
    /// Destructures and returns any typed data.
    pub fn if_typed(self) -> Option<T> {
        match self.payload {
            MessageContents::Binary(_) => None,
            MessageContents::Owned(typed) => Some(typed),
            MessageContents::Arc(_) => None,
        }
    }
    /// Returns a mutable reference, if typed.
    pub fn if_mut(&mut self) -> Option<&mut T> {
        match &mut self.payload {
            MessageContents::Binary(_) => None,
            MessageContents::Owned(typed) => Some(typed),
            MessageContents::Arc(_) => None,
        }
    }
    /// Returns an immutable or mutable typed reference.
    ///
    /// This method returns a mutable reference if the underlying data are typed Rust
    /// instances, which admit mutation, and it returns an immutable reference if the
    /// data are serialized binary data.
    pub fn as_ref_or_mut(&mut self) -> RefOrMut<T> {
        match &mut self.payload {
            MessageContents::Binary(bytes) => { RefOrMut::Ref(bytes) },
            MessageContents::Owned(typed) => { RefOrMut::Mut(typed) },
            MessageContents::Arc(typed) => { RefOrMut::Ref(typed) },
        }
    }
}

// These methods require `T` to implement `Abomonation`, for serialization functionality.
#[cfg(not(feature = "bincode"))]
impl<T: Data> Message<T> {
    /// Wrap bytes as a message.
    ///
    /// # Safety
    ///
    /// This method is unsafe, in that `Abomonated::new()` is unsafe: it presumes that
    /// the binary data can be safely decoded, which is unsafe for e.g. UTF8 data and
    /// enumerations (perhaps among many other types).
    pub unsafe fn from_bytes(bytes: Bytes) -> Self {
        let abomonated = abomonation::abomonated::Abomonated::new(bytes).expect("Abomonated::new() failed.");
        Message { payload: MessageContents::Binary(abomonated) }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        match &self.payload {
            MessageContents::Binary(bytes) => { bytes.as_bytes().len() },
            MessageContents::Owned(typed) => { abomonation::measure(typed) },
            MessageContents::Arc(typed) =>{ abomonation::measure::<T>(&**typed) } ,
        }
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match &self.payload {
            MessageContents::Binary(bytes) => {
                writer.write_all(bytes.as_bytes()).expect("Message::into_bytes(): write_all failed.");
            },
            MessageContents::Owned(typed) => {
                unsafe { abomonation::encode(typed, writer).expect("Message::into_bytes(): Abomonation::encode failed"); }
            },
            MessageContents::Arc(typed) => {
                unsafe { abomonation::encode(&**typed, writer).expect("Message::into_bytes(): Abomonation::encode failed"); }
            },
        }
    }
}

#[cfg(feature = "bincode")]
impl<T: Data> Message<T> {
    /// Wrap bytes as a message.
    pub fn from_bytes(bytes: Bytes) -> Self {
        let typed = ::bincode::deserialize(&bytes[..]).expect("bincode::deserialize() failed");
        Message { payload: MessageContents::Owned(typed) }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        match &self.payload {
            MessageContents::Binary(bytes) => { bytes.as_bytes().len() },
            MessageContents::Owned(typed) => {
                ::bincode::serialized_size(&typed).expect("bincode::serialized_size() failed") as usize
            },
            MessageContents::Arc(typed) => {
                ::bincode::serialized_size(&**typed).expect("bincode::serialized_size() failed") as usize
            },
        }
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match &self.payload {
            MessageContents::Binary(bytes) => {
                writer.write_all(bytes.as_bytes()).expect("Message::into_bytes(): write_all failed.");
            },
            MessageContents::Owned(typed) => {
                ::bincode::serialize_into(writer, &typed).expect("bincode::serialize_into() failed");
            },
            MessageContents::Arc(typed) => {
                ::bincode::serialize_into(writer, &**typed).expect("bincode::serialize_into() failed");
            },
        }
    }
}

impl<T> ::std::ops::Deref for Message<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // TODO: In principle we have aready decoded, but let's go again
        match &self.payload {
            MessageContents::Binary(bytes) => { bytes },
            MessageContents::Owned(typed) => { typed },
            MessageContents::Arc(typed) => { typed },
        }
    }
}

impl<T: Clone> Message<T> {
    /// Produces a typed instance of the wrapped element.
    pub fn into_typed(self) -> T {
        match self.payload {
            MessageContents::Binary(bytes) => bytes.clone(),
            MessageContents::Owned(instance) => instance,
            // TODO: Could attempt `Arc::try_unwrap()` here.
            MessageContents::Arc(instance) => (*instance).clone(),
        }
    }
    /// Ensures the message is typed data and returns a mutable reference to it.
    pub fn as_mut(&mut self) -> &mut T {

        let cloned: Option<T> = match &self.payload {
            MessageContents::Binary(bytes) => Some((*bytes).clone()),
            MessageContents::Owned(_) => None,
            // TODO: Could attempt `Arc::try_unwrap()` here.
            MessageContents::Arc(typed) => Some((**typed).clone()),
        };

        if let Some(cloned) = cloned {
            self.payload = MessageContents::Owned(cloned);
        }

        if let MessageContents::Owned(typed) = &mut self.payload {
            typed
        }
        else {
            unreachable!()
        }
    }
}
