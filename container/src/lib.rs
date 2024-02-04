//! Specifications for containers

#![forbid(missing_docs)]

pub mod columnation;
pub mod flatcontainer;

/// A container transferring data through dataflow edges
///
/// A container stores a number of elements and thus is able to describe it length (`len()`) and
/// whether it is empty (`is_empty()`). It supports removing all elements (`clear`).
///
/// A container must implement default. The default implementation is not required to allocate
/// memory for variable-length components.
///
/// We require the container to be cloneable to enable efficient copies when providing references
/// of containers to operators. Care must be taken that the type's `clone_from` implementation
/// is efficient (which is not necessarily the case when deriving `Clone`.)
/// TODO: Don't require `Container: Clone`
pub trait Container: Default + Clone + 'static {
    /// The type of elements when reading non-destructively from the container.
    type ItemRef<'a> where Self: 'a;

    /// The type of elements when draining the continer.
    type Item<'a> where Self: 'a;

    /// The number of elements in this container
    ///
    /// The length of a container must be consistent between sending and receiving it.
    /// When exchanging a container and partitioning it into pieces, the sum of the length
    /// of all pieces must be equal to the length of the original container. When combining
    /// containers, the length of the result must be the sum of the individual parts.
    fn len(&self) -> usize;

    /// Determine if the container contains any elements, corresponding to `len() == 0`.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all contents from `self` while retaining allocated memory.
    /// After calling `clear`, `is_empty` must return `true` and `len` 0.
    fn clear(&mut self);

    /// Iterator type when reading from the container.
    type Iter<'a>: Iterator<Item=Self::ItemRef<'a>>;

    /// Returns an iterator that reads the contents of this container.
    fn iter(&self) -> Self::Iter<'_>;

    /// Iterator type when draining the container.
    type DrainIter<'a>: Iterator<Item=Self::Item<'a>>;

    /// Returns an iterator that drains the contents of this container.
    /// Drain leaves the container in an undefined state.
    fn drain(&mut self) -> Self::DrainIter<'_>;
}

/// A type that can push itself into a container.
pub trait PushInto<C> {
    /// Push self into the target container.
    fn push_into(self, target: &mut C);
}

/// A type that has the necessary infrastructure to push elements, without specifying how pushing
/// itself works. For this, pushable types should implement [`PushInto`].
// TODO: Reconsider this interface because it assumes
//   * Containers have a capacity
//   * Push presents single elements.
//   * Instead of testing `len == cap`, we could have a `is_full` to test that we might
//     not be able to absorb more data.
//   * Example: A FlatStack with optimized offsets and deduplication can absorb many elements without reallocation. What does capacity mean in this context?
pub trait PushContainer: Container {
    /// Push `item` into self
    #[inline]
    fn push<T: PushInto<Self>>(&mut self, item: T) {
        item.push_into(self)
    }
    /// Return the capacity of the container.
    fn capacity(&self) -> usize;
    /// Return the preferred capacity of the container.
    fn preferred_capacity() -> usize;
    /// Reserve space for `additional` elements, possibly increasing the capacity of the container.
    fn reserve(&mut self, additional: usize);
}

impl<T: Clone + 'static> Container for Vec<T> {
    type ItemRef<'a> = &'a T where T: 'a;
    type Item<'a> = T where T: 'a;

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn clear(&mut self) { Vec::clear(self) }

    type Iter<'a> = std::slice::Iter<'a, T>;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }

    type DrainIter<'a> = std::vec::Drain<'a, T>;

    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.drain(..)
    }
}

impl<T: Clone + 'static> PushContainer for Vec<T> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn preferred_capacity() -> usize {
        buffer::default_capacity::<T>()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional);
    }
}

impl<T> PushInto<Vec<T>> for T {
    #[inline]
    fn push_into(self, target: &mut Vec<T>) {
        target.push(self)
    }
}

impl<'a, T: Clone> PushInto<Vec<T>> for &'a T {
    #[inline]
    fn push_into(self, target: &mut Vec<T>) {
        target.push(self.clone())
    }
}

mod rc {
    use std::ops::Deref;
    use std::rc::Rc;

    use crate::Container;

    impl<T: Container> Container for Rc<T> {
        type ItemRef<'a> = T::ItemRef<'a> where Self: 'a;
        type Item<'a> = T::ItemRef<'a> where Self: 'a;

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }

        fn clear(&mut self) {
            // Try to reuse the allocation if possible
            if let Some(inner) = Rc::get_mut(self) {
                inner.clear();
            } else {
                *self = Self::default();
            }
        }

        type Iter<'a> = T::Iter<'a>;

        fn iter(&self) -> Self::Iter<'_> {
            self.deref().iter()
        }

        type DrainIter<'a> = T::Iter<'a>;

        fn drain(&mut self) -> Self::DrainIter<'_> {
            self.iter()
        }
    }
}

mod arc {
    use std::ops::Deref;
    use std::sync::Arc;

    use crate::Container;

    impl<T: Container> Container for Arc<T> {
        type ItemRef<'a> = T::ItemRef<'a> where Self: 'a;
        type Item<'a> = T::ItemRef<'a> where Self: 'a;

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }

        fn clear(&mut self) {
            // Try to reuse the allocation if possible
            if let Some(inner) = Arc::get_mut(self) {
                inner.clear();
            } else {
                *self = Self::default();
            }
        }

        type Iter<'a> = T::Iter<'a>;

        fn iter(&self) -> Self::Iter<'_> {
            self.deref().iter()
        }

        type DrainIter<'a> = T::Iter<'a>;

        fn drain(&mut self) -> Self::DrainIter<'_> {
            self.iter()
        }
    }
}

/// A container that can partition itself into pieces.
pub trait PushPartitioned: PushContainer {
    /// Partition and push this container.
    ///
    /// Drain all elements from `self`, and use the function `index` to determine which `buffer` to
    /// append an element to. Call `flush` with an index and a buffer to send the data downstream.
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], index: I, flush: F)
    where
        for<'a> I: FnMut(&Self::Item<'a>) -> usize,
        F: FnMut(usize, &mut Self);
}

impl<T: PushContainer + 'static> PushPartitioned for T where for<'a> T::Item<'a>: PushInto<T> {
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], mut index: I, mut flush: F)
    where
        for<'a> I: FnMut(&Self::Item<'a>) -> usize,
        F: FnMut(usize, &mut Self),
    {
        let ensure_capacity = |this: &mut Self| {
            let capacity = this.capacity();
            let desired_capacity = Self::preferred_capacity();
            if capacity < desired_capacity {
                this.reserve(desired_capacity - capacity);
            }
        };

        for datum in self.drain() {
            let index = index(&datum);
            ensure_capacity(&mut buffers[index]);
            buffers[index].push(datum);
            if buffers[index].len() >= buffers[index].capacity() {
                flush(index, &mut buffers[index]);
            }
        }
        self.clear();
    }
}

pub mod buffer {
    //! Functionality related to calculating default buffer sizes

    /// The upper limit for buffers to allocate, size in bytes. [default_capacity] converts
    /// this to size in elements.
    pub const BUFFER_SIZE_BYTES: usize = 1 << 13;

    /// The maximum buffer capacity in elements. Returns a number between [BUFFER_SIZE_BYTES]
    /// and 1, inclusively.
    pub const fn default_capacity<T>() -> usize {
        let size = ::std::mem::size_of::<T>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }
}
