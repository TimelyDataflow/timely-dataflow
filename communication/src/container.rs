//! Specifications for containers and allocations

use std::num::*;

use paste::paste;

use crate::message::RefOrMut;

/// A container transferring data through dataflow edges
///
/// Each container has a corresponding allocation type. The allocation type
/// captures any memory allocations the container owned after its contents
/// have been consumed. A consumed container can be converted into its
/// allocation type using `hollow()`.
pub trait Container: Sized {

    /// The allocation type capturing any allocations this container might own.
    type Allocation: IntoAllocated<Self> + 'static;
    // type Allocation: 'static;

    /// Convert an allocated container into the allocation type.
    ///
    /// The allocation should not hold any usable data. Container types have to be cleared.
    fn hollow(self) -> Self::Allocation;

    /// The number of elements in this container for the purpose of progress tracking
    fn len(&self) -> usize;

    /// Determine if the container contains any elements, corresponding to `len() == 0`.
    fn is_empty(&self) -> bool;

    /// Ensure the containers capacity is allocated and reallocate if needed.
    fn ensure_capacity(&mut self) { }
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
        true
    }
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

    fn ensure_capacity(&mut self) {
        let len = self.len();
        if len < 1024 {
            self.reserve(1024 - len);
        }
    }
}

/// Turn a hollow allocation into an allocated object.
pub trait IntoAllocated<T> {
    /// Use the allocation in `self` to reconstruct the allocated object into an
    /// owned object.
    ///
    /// The implementation is free to modify mutable references.
    #[inline(always)]
    fn assemble(self, allocated: RefOrMut<T>) -> T where Self: Sized {
        Self::assemble_new(allocated)
    }

    /// Construct a new owned object from a reference to an allocation.
    ///
    /// The implementation is free to modify mutable references.
    fn assemble_new(allocated: RefOrMut<T>) -> T;
}


impl IntoAllocated<()> for () {
    fn assemble_new(_allocated: RefOrMut<()>) -> () {
        ()
    }
}

impl<T: Clone> IntoAllocated<Vec<T>> for Vec<T> {
    #[inline(always)]
    fn assemble(mut self, ref_or_mut: RefOrMut<Vec<T>>) -> Vec<T> {
        match ref_or_mut {
            RefOrMut::Ref(t) => {
                self.clone_from(t);
            }
            RefOrMut::Mut(t) => {
                ::std::mem::swap(&mut self, t);
            },
        }
        self
    }

    #[inline(always)]
    fn assemble_new(ref_or_mut: RefOrMut<Vec<T>>) -> Vec<T> {
        Self::new().assemble(ref_or_mut)
    }
}

impl IntoAllocated<String> for String {
    #[inline(always)]
    fn assemble(mut self, allocated: RefOrMut<String>) -> String where Self: Sized {
        match allocated {
            RefOrMut::Ref(t) => self.clone_from(t),
            RefOrMut::Mut(t) => ::std::mem::swap(&mut self, t),
        }
        self
    }

    #[inline(always)]
    fn assemble_new(allocated: RefOrMut<String>) -> String {
        Self::with_capacity(allocated.len()).assemble(allocated)
    }
}

macro_rules! implement_clone_container {
    ($($index_type:ty,)*) => (
        $(
            impl Container for $index_type {
                type Allocation = ();
                fn hollow(self) -> Self::Allocation { () }
                fn len(&self) -> usize { 0 }
                fn is_empty(&self) -> bool { true }
            }
            impl IntoAllocated<$index_type> for () {
                #[inline(always)]
                fn assemble_new(allocated: RefOrMut<$index_type>) -> $index_type { (&*allocated).clone() }
            }
        )*
    )
}

implement_clone_container!(usize, u128, u64, u32, u16, u8, isize, i128, i64, i32, i16, i8, ::std::time::Duration,);
implement_clone_container!(f32, f64,);
implement_clone_container!(NonZeroU8, NonZeroI8, NonZeroU16, NonZeroI16, NonZeroU32, NonZeroI32,);
implement_clone_container!(NonZeroU64, NonZeroI64, NonZeroU128, NonZeroI128, NonZeroUsize, NonZeroIsize,);

// general code for tuples (can't use '0', '1', ... as field identifiers)
macro_rules! tuple_container {
    ( $($name:ident)+) => ( paste! {
        impl<$($name: Container),*> Container for ($($name,)*) {
            type Allocation = ($($name::Allocation,)*);
            fn hollow(self) -> Self::Allocation {
                #[allow(non_snake_case)]
                let ($($name,)*) = self;
                ($($name.hollow(),)*)
            }
            fn len(&self) -> usize {
                let mut len = 0;
                #[allow(non_snake_case)]
                let ($(ref $name,)*) = *self;
                $( len += $name.len(); )*
                len
            }

            fn is_empty(&self) -> bool {
                let mut is_empty = true;
                #[allow(non_snake_case)]
                let ($(ref $name,)*) = *self;
                $( is_empty = is_empty && $name.is_empty(); )*
                is_empty
            }

        }
        impl<$($name: Container),*> IntoAllocated<($($name,)*)> for ($($name::Allocation,)*)
            where $($name::Allocation: IntoAllocated<$name>,)*
        {
            fn assemble(self, allocated: RefOrMut<($($name,)*)>) -> ($($name,)*) where Self: Sized {
                match allocated {
                    RefOrMut::Ref(t) => {
                        #[allow(non_snake_case)]
                        let ($(ref $name,)*) = t;
                        #[allow(non_snake_case)]
                        let ($([<self_ $name>],)*) = self;
                        ($([<self_ $name>].assemble(RefOrMut::Ref($name)),)*)
                    },
                    RefOrMut::Mut(t) => {
                        #[allow(non_snake_case)]
                        let ($(ref mut $name,)*) = t;
                        #[allow(non_snake_case)]
                        let ($([<self_ $name >],)*) = self;
                        ($([<self_ $name>].assemble(RefOrMut::Mut($name)),)*)
                    }
                }
            }

            fn assemble_new(allocated: RefOrMut<($($name,)*)>) -> ($($name,)*) {
                match allocated {
                    RefOrMut::Ref(t) => {
                        #[allow(non_snake_case)]
                        let ($(ref [<t_ $name>],)*) = t;
                        ($( $name::Allocation::assemble_new(RefOrMut::Ref([<t_ $name >])),)*)
                    },
                    RefOrMut::Mut(t) => {
                        #[allow(non_snake_case)]
                        let ($(ref mut [<t_ $name >],)*) = t;
                        ($( $name::Allocation::assemble_new(RefOrMut::Mut([<t_ $name >])),)*)
                    },
                }
            }
        }
        }
    );
}

tuple_container!(A);
tuple_container!(A B);
tuple_container!(A B C);
tuple_container!(A B C D);
tuple_container!(A B C D E);
tuple_container!(A B C D E F);
tuple_container!(A B C D E F G);
tuple_container!(A B C D E F G H);
tuple_container!(A B C D E F G H I);
tuple_container!(A B C D E F G H I J);
tuple_container!(A B C D E F G H I J K);
tuple_container!(A B C D E F G H I J K L);
tuple_container!(A B C D E F G H I J K L M);
tuple_container!(A B C D E F G H I J K L M N);
tuple_container!(A B C D E F G H I J K L M N O);
tuple_container!(A B C D E F G H I J K L M N O P);
tuple_container!(A B C D E F G H I J K L M N O P Q);
tuple_container!(A B C D E F G H I J K L M N O P Q R);
tuple_container!(A B C D E F G H I J K L M N O P Q R S);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE);
tuple_container!(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE AF);


mod rc {
    use std::rc::Rc;

    use crate::{Container, IntoAllocated};
    use crate::message::RefOrMut;

    impl<T: Container> Container for Rc<T> {
        type Allocation = ();

        fn hollow(self) -> Self::Allocation {
            ()
        }

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }
    }

    impl<T: Container> IntoAllocated<Rc<T>> for () {
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
