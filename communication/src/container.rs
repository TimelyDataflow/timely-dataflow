//! Specifications for containers and allocations

use crate::message::RefOrMut;

/// TODO
pub trait Container: Sized {

    /// TODO
    type Allocation: IntoAllocated<Self> + 'static;
    // type Allocation: 'static;

    /// TODO
    fn hollow(self) -> Self::Allocation;

    /// TODO
    fn len(&self) -> usize;

    ///
    fn is_empty(&self) -> bool;
}

impl Container for () {
    type Allocation = ();
    fn hollow(self) -> Self::Allocation {
        ()
    }

    fn len(&self) -> usize {
        0
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<A: Container> Container for (A,) {
    type Allocation = (A::Allocation,);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), )
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<A: Container, B: Container> Container for (A, B) {
    type Allocation = (A::Allocation, B::Allocation);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), self.1.hollow())
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty()
    }
}

impl<A: Container, B: Container, C: Container> Container for (A, B, C) {
    type Allocation = (A::Allocation, B::Allocation, C::Allocation);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), self.1.hollow(), self.2.hollow())
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len() + self.2.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty() && self.2.is_empty()
    }
}

impl<A: Container, B: Container, C: Container, D: Container> Container for (A, B, C, D) {
    type Allocation = (A::Allocation, B::Allocation, C::Allocation, D::Allocation);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), self.1.hollow(), self.2.hollow(), self.3.hollow())
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len() + self.2.len() + self.3.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty() && self.2.is_empty() && self.3.is_empty()
    }
}

impl Container for ::std::time::Duration {
    type Allocation = ();
    fn hollow(self) -> Self::Allocation { () }
    fn len(&self) -> usize { 0 }
    fn is_empty(&self) -> bool { false }
}

impl Container for String {
    type Allocation = String;

    fn hollow(mut self) -> Self::Allocation {
        self.clear();
        self
    }

    fn len(&self) -> usize {
        String::len(self)
    }

    fn is_empty(&self) -> bool {
        String::is_empty(self)
    }
}

impl<T: Clone + 'static> Container for Vec<T> {
    type Allocation = Self;

    fn hollow(mut self) -> Self::Allocation {
        self.clear();
        self
    }

    fn len(&self) -> usize {
        Vec::len(&self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(&self)
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

impl IntoAllocated<::std::time::Duration> for () {
    fn assemble_new(allocated: RefOrMut<::std::time::Duration>) -> ::std::time::Duration {
        (&*allocated).clone()
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

macro_rules! implement_container {
    ($($index_type:ty,)*) => (
        $(
            impl Container for $index_type {
                type Allocation = ();
                fn hollow(self) -> Self::Allocation { () }
                fn len(&self) -> usize { 0 }
                fn is_empty(&self) -> bool { true }
            }
            impl IntoAllocated<$index_type> for () {
                fn assemble_new(allocated: RefOrMut<$index_type>) -> $index_type { *allocated }
            }
        )*
    )
}

implement_container!(usize, u128, u64, u32, u16, u8, isize, i128, i64, i32, i16, i8,);

mod rc {
    use std::rc::Rc;

    use crate::IntoAllocated;
    use crate::message::RefOrMut;

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
    use crate::{IntoAllocated, Container};
    use crate::message::RefOrMut;

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
        assert_eq!(t.len(), 8);
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
