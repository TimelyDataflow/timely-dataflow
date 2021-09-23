//! Types wrapping typed data.

use std::sync::Arc;
use bytes::arc::Bytes;
use abomonation;
use crate::{Data, Container};

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

    // pub fn take(self) -> T {
    //     match self {
    //         RefOrMut::Ref(reference) => element.clone_from(reference),
    //         RefOrMut::Mut(reference) => ::std::mem::swap(reference, element),
    //     }
    // };

    /// TODO
    pub fn assemble<A: IntoAllocated<T>>(self, allocation: &mut Option<A>) -> T {
        if let Some(allocation) = allocation.take() {
            allocation.assemble_ref(&*self)
        } else {
            A::assemble_new_ref(&*self)
        }
    }
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

/// TODO
pub struct MessageAllocation<T>(T);

impl<T: Container> Container for Message<T> {
    type Allocation = MessageAllocation<T::Allocation>;
    fn hollow(self) -> Option<Self::Allocation> {
        match self.payload {
            MessageContents::Binary(_) | MessageContents::Arc(_) => None,
            MessageContents::Owned(allocated) => allocated.hollow().map(|allocation| MessageAllocation(allocation)),
        }
    }
}

// /// A hollow allocation
// pub trait FromAllocated<A> {
//     /// Turn an allocated object into a hollow object, if possible
//     fn hollow(self) -> Option<A>;
// }
//
// impl<T, A: From<T>> FromAllocated<A> for Message<T> {
//     fn hollow(self) -> Option<A> {
//         match self.payload {
//             MessageContents::Binary(_) | MessageContents::Arc(_) => None,
//             MessageContents::Owned(allocated) => Some(allocated.into()),
//         }
//     }
// }
//
// impl<T> FromAllocated<Vec<T>> for Vec<T> {
//     fn hollow(mut self) -> Option<Vec<T>> {
//         self.clear();
//         Some(self)
//     }
// }

/// TODO
pub trait IntoAllocated<T> {
    /// TODO
    fn assemble(self, allocated: T) -> T where Self: Sized {
        Self::assemble_new(allocated)
    }
    /// TODO
    fn assemble_new(allocated: T) -> T;
}

impl<T: Container> IntoAllocated<Message<T>> for MessageAllocation<T::Allocation> {
    fn assemble(self, ref_or_mut: Message<T>) -> Message<T> where Self: Sized {
        match ref_or_mut.payload {
            MessageContents::Binary(binary) => Message { payload: MessageContents::Binary(binary) },
            MessageContents::Owned(owned) => Message::from_typed(T::Allocation::assemble(self.0, owned)),
            MessageContents::Arc(arc) => Message::from_arc(arc),
        }
    }

    fn assemble_new(ref_or_mut: Message<T>) -> Message<T> {
        todo!()
    }

    fn assemble_ref(self, allocated: &Message<T>) -> Message<T> where Message<T>: Clone {
        todo!()
    }

    fn assemble_new_ref(allocated: &Message<T>) -> Message<T> where Message<T>: Clone {
        todo!()
    }
}

impl IntoAllocated<()> for () {
    fn assemble_new(allocated: ()) -> () {
        ()
    }

    fn assemble_ref(self, allocated: &()) -> () where (): Clone {
        ()
    }

    fn assemble_new_ref(allocated: &()) -> () where (): Clone {
        ()
    }
}

impl<A: Container> IntoAllocated<(A,)> for (A::Allocation,) {
    fn assemble_new(allocated: (A::Allocation,)) -> (A,) {
        (A::Allocation::assemble_new(allocated.0),)
    }

    fn assemble_ref(self, allocated: &(A, )) -> (A, ) where (A, ): Clone {
        todo!()
    }

    fn assemble_new_ref(allocated: &(A, )) -> (A, ) where (A, ): Clone {
        todo!()
    }
}

impl<'a, T: Clone> IntoAllocated<Vec<T>> for Vec<T> {
    fn assemble(mut self, ref_or_mut: Vec<T>) -> Vec<T> {
        self.clone_from(&ref_or_mut);
        self
    }

    fn assemble_new(ref_or_mut: Vec<T>) -> Vec<T> {
        let mut vec = Vec::new();
        vec.clone_from(&ref_or_mut);
        vec
    }

    fn assemble_ref(self, allocated: &Vec<T>) -> Vec<T> where Vec<T>: Clone {
        todo!()
    }

    fn assemble_new_ref(allocated: &Vec<T>) -> Vec<T> where Vec<T>: Clone {
        todo!()
    }
}

mod rc {
    use crate::message::{IntoAllocated, RefOrMut};
    use std::rc::Rc;

    impl<T> IntoAllocated<Rc<T>> for () {
        fn assemble(self, ref_or_mut: Rc<T>) -> Rc<T> {
            Rc::clone(&ref_or_mut)
        }

        fn assemble_new(ref_or_mut: Rc<T>) -> Rc<T> {
            Rc::clone(&ref_or_mut)
        }

        fn assemble_ref(self, allocated: &Rc<T>) -> Rc<T> where Rc<T>: Clone {
            todo!()
        }

        fn assemble_new_ref(allocated: &Rc<T>) -> Rc<T> where Rc<T>: Clone {
            todo!()
        }
    }
}
