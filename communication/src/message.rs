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
}

impl<'a, T: 'a> RefOrMut<'a, T> {
    /// TODO
    pub fn assemble<A: IntoAllocated<T>>(self, allocation: &mut Option<A>) -> T {
        if let Some(allocation) = allocation.take() {
            allocation.assemble(self)
        } else {
            A::assemble_new(self)
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
pub struct MessageAllocation<T>(pub T);

impl<T: Container> Container for Message<T> {
    type Allocation = MessageAllocation<Option<T::Allocation>>;
    fn hollow(self) -> Self::Allocation {
        match self.payload {
            MessageContents::Binary(_) | MessageContents::Arc(_) => MessageAllocation(None),
            MessageContents::Owned(allocated) => MessageAllocation(Some(allocated.hollow()))
        }
    }

    fn len(&self) -> usize {
        T::len(&*self)
    }

    fn is_empty(&self) -> bool {
        T::is_empty(&*self)
    }
}

/// Turn a hollow allocation into an allocated object.
pub trait IntoAllocated<T> {
    /// Use the allocation in `self` to reconstruct the allocated object into an
    /// owned object.
    fn assemble(self, allocated: RefOrMut<T>) -> T where Self: Sized {
        Self::assemble_new(allocated)
    }
    /// TODO
    fn assemble_new(allocated: RefOrMut<T>) -> T;
}

impl<T: Container> IntoAllocated<Message<T>> for MessageAllocation<Option<T::Allocation>> {
    fn assemble_new(_allocated: RefOrMut<Message<T>>) -> Message<T> {
        unreachable!()
    }
}

impl IntoAllocated<()> for () {
    fn assemble_new(_allocated: RefOrMut<()>) -> () {
        ()
    }
}

impl<A: Container> IntoAllocated<(A, )> for (A::Allocation, )
where
    A::Allocation: IntoAllocated<A>,
{
    fn assemble(self, allocated: RefOrMut<(A, )>) -> (A, ) where Self: Sized {
        match allocated {
            RefOrMut::Ref(t) => (self.0.assemble(RefOrMut::Ref(&t.0)), ),
            RefOrMut::Mut(t) => (self.0.assemble(RefOrMut::Mut(&mut t.0)), ),
        }
    }

    fn assemble_new(allocated: RefOrMut<(A, )>) -> (A, ) {
        match allocated {
            RefOrMut::Ref(t) => (A::Allocation::assemble_new(RefOrMut::Ref(&t.0)), ),
            RefOrMut::Mut(t) => (A::Allocation::assemble_new(RefOrMut::Mut(&mut t.0)), ),
        }
    }
}

impl<A: Container, B: Container> IntoAllocated<(A, B)> for (A::Allocation, B::Allocation)
    where
        A::Allocation: IntoAllocated<A>,
        B::Allocation: IntoAllocated<B>,
{
    fn assemble(self, allocated: RefOrMut<(A, B)>) -> (A, B) where Self: Sized {
        match allocated {
            RefOrMut::Ref(t) => (
                self.0.assemble(RefOrMut::Ref(&t.0)),
                self.1.assemble(RefOrMut::Ref(&t.1))
            ),
            RefOrMut::Mut(t) => (
                self.0.assemble(RefOrMut::Mut(&mut t.0)),
                self.1.assemble(RefOrMut::Mut(&mut t.1))
            ),
        }
    }

    fn assemble_new(allocated: RefOrMut<(A, B)>) -> (A, B) {
        match allocated {
            RefOrMut::Ref(t) => (
                A::Allocation::assemble_new(RefOrMut::Ref(&t.0)),
                B::Allocation::assemble_new(RefOrMut::Ref(&t.1))
            ),
            RefOrMut::Mut(t) => (
                A::Allocation::assemble_new(RefOrMut::Mut(&mut t.0)),
                B::Allocation::assemble_new(RefOrMut::Mut(&mut t.1)),
            ),
        }
    }
}

impl<A: Container, B: Container, C: Container> IntoAllocated<(A, B, C)> for (A::Allocation, B::Allocation, C::Allocation)
    where
        A::Allocation: IntoAllocated<A>,
        B::Allocation: IntoAllocated<B>,
        C::Allocation: IntoAllocated<C>,
{
    fn assemble(self, allocated: RefOrMut<(A, B, C)>) -> (A, B, C) where Self: Sized {
        match allocated {
            RefOrMut::Ref(t) => (
                self.0.assemble(RefOrMut::Ref(&t.0)),
                self.1.assemble(RefOrMut::Ref(&t.1)),
                self.2.assemble(RefOrMut::Ref(&t.2)),
            ),
            RefOrMut::Mut(t) => (
                self.0.assemble(RefOrMut::Mut(&mut t.0)),
                self.1.assemble(RefOrMut::Mut(&mut t.1)),
                self.2.assemble(RefOrMut::Mut(&mut t.2)),
            ),
        }
    }

    fn assemble_new(allocated: RefOrMut<(A, B, C)>) -> (A, B, C) {
        match allocated {
            RefOrMut::Ref(t) => (
                A::Allocation::assemble_new(RefOrMut::Ref(&t.0)),
                B::Allocation::assemble_new(RefOrMut::Ref(&t.1)),
                C::Allocation::assemble_new(RefOrMut::Ref(&t.2)),
            ),
            RefOrMut::Mut(t) => (
                A::Allocation::assemble_new(RefOrMut::Mut(&mut t.0)),
                B::Allocation::assemble_new(RefOrMut::Mut(&mut t.1)),
                C::Allocation::assemble_new(RefOrMut::Mut(&mut t.2)),
            ),
        }
    }
}

impl<A: Container, B: Container, C: Container, D: Container> IntoAllocated<(A, B, C, D)> for (A::Allocation, B::Allocation, C::Allocation, D::Allocation)
    where
        A::Allocation: IntoAllocated<A>,
        B::Allocation: IntoAllocated<B>,
        C::Allocation: IntoAllocated<C>,
        D::Allocation: IntoAllocated<D>,
{
    fn assemble(self, allocated: RefOrMut<(A, B, C, D)>) -> (A, B, C, D) where Self: Sized {
        match allocated {
            RefOrMut::Ref(t) => (
                self.0.assemble(RefOrMut::Ref(&t.0)),
                self.1.assemble(RefOrMut::Ref(&t.1)),
                self.2.assemble(RefOrMut::Ref(&t.2)),
                self.3.assemble(RefOrMut::Ref(&t.3)),
            ),
            RefOrMut::Mut(t) => (
                self.0.assemble(RefOrMut::Mut(&mut t.0)),
                self.1.assemble(RefOrMut::Mut(&mut t.1)),
                self.2.assemble(RefOrMut::Mut(&mut t.2)),
                self.3.assemble(RefOrMut::Mut(&mut t.3)),
            ),
        }
    }

    fn assemble_new(allocated: RefOrMut<(A, B, C, D)>) -> (A, B, C, D) {
        match allocated {
            RefOrMut::Ref(t) => (
                A::Allocation::assemble_new(RefOrMut::Ref(&t.0)),
                B::Allocation::assemble_new(RefOrMut::Ref(&t.1)),
                C::Allocation::assemble_new(RefOrMut::Ref(&t.2)),
                D::Allocation::assemble_new(RefOrMut::Ref(&t.3)),
            ),
            RefOrMut::Mut(t) => (
                A::Allocation::assemble_new(RefOrMut::Mut(&mut t.0)),
                B::Allocation::assemble_new(RefOrMut::Mut(&mut t.1)),
                C::Allocation::assemble_new(RefOrMut::Mut(&mut t.2)),
                D::Allocation::assemble_new(RefOrMut::Mut(&mut t.3)),
            ),
        }
    }
}

impl<T: Clone> IntoAllocated<Vec<T>> for Vec<T> {
    fn assemble(mut self, ref_or_mut: RefOrMut<Vec<T>>) -> Vec<T> {
        match ref_or_mut {
            RefOrMut::Ref(t) => {
                self.clone_from(t);
                println!("into_allocated<Vec<T>> ref")
            }
            RefOrMut::Mut(t) => {
                ::std::mem::swap(&mut self, t);
                println!("into_allocated<Vec<T>> mut")
            },
        }
        self
    }

    fn assemble_new(ref_or_mut: RefOrMut<Vec<T>>) -> Vec<T> {
        Self::new().assemble(ref_or_mut)
    }
}

impl IntoAllocated<usize> for () {
    fn assemble_new(allocated: RefOrMut<usize>) -> usize {
        *allocated
    }
}

impl IntoAllocated<String> for String {
    fn assemble(mut self, allocated: RefOrMut<String>) -> String where Self: Sized {
        match allocated {
            RefOrMut::Ref(t) => self.clone_from(t),
            RefOrMut::Mut(t) => ::std::mem::swap(&mut self, t),
        }
        self
    }

    fn assemble_new(allocated: RefOrMut<String>) -> String {
        Self::with_capacity(allocated.len()).assemble(allocated)
    }
}

mod rc {
    use crate::message::{IntoAllocated, RefOrMut};
    use std::rc::Rc;

    impl<T> IntoAllocated<Rc<T>> for () {
        fn assemble(self, ref_or_mut: RefOrMut<Rc<T>>) -> Rc<T> {
            Rc::clone(&*ref_or_mut)
        }

        fn assemble_new(ref_or_mut: RefOrMut<Rc<T>>) -> Rc<T> {
            Rc::clone(&*ref_or_mut)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::Container;
    use crate::message::{IntoAllocated, RefOrMut};

    #[test]
    fn test_container_ref() {
        let c = vec![1, 2, 3, 4];
        let data = c.hollow().assemble(RefOrMut::Ref(&vec![5, 6, 7, 8]));
        assert_eq!(data, vec![5, 6, 7, 8]);
    }

    #[test]
    fn test_container_mut() {
        let c = vec![1, 2, 3, 4];
        let data = c.hollow().assemble(RefOrMut::Mut(&mut vec![5, 6, 7, 8]));
        assert_eq!(data, vec![5, 6, 7, 8]);
    }

    #[test]
    fn test_tuple_len() {
        let t = (1, vec![1, 2, 3], "Hello".to_string());
        assert_eq!(t.len(), 9);
    }

    #[test]
    fn test_vec_hollow() {
        let c = vec![1, 2, 3, 4].hollow();
        assert!(c.is_empty());
        assert_eq!(c.capacity(), 4);
    }

    #[test]
    fn test_vec_assemble_ref() {
        let c = vec![1, 2, 3, 4].hollow();
        let c = c.assemble(RefOrMut::Ref(&vec![5, 6]));
        assert!(!c.is_empty());
        assert_eq!(c.capacity(), 4);
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn test_vec_assemble_mut() {
        let c = vec![1, 2, 3, 4].hollow();
        let c = c.assemble(RefOrMut::Mut(&mut vec![5, 6]));
        assert!(!c.is_empty());
        assert_eq!(c.capacity(), 2);
        assert_eq!(c.len(), 2);
    }
}
