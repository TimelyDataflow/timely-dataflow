//! Specifications for containers

#![forbid(missing_docs)]

use std::collections::VecDeque;

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

    /// The type of elements when draining the container.
    type Item<'a> where Self: 'a;

    /// Push `item` into self
    #[inline]
    fn push<T>(&mut self, item: T) where Self: PushInto<T> {
        self.push_into(item)
    }

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

/// A container that can be sized and reveals its capacity.
pub trait SizableContainer: Container {
    /// Return the capacity of the container.
    fn capacity(&self) -> usize;
    /// Return the preferred capacity of the container.
    fn preferred_capacity() -> usize;
    /// Reserve space for `additional` elements, possibly increasing the capacity of the container.
    fn reserve(&mut self, additional: usize);
}

/// A container that can absorb items of a specific type.
pub trait PushInto<T> {
    /// Push item into self.
    fn push_into(&mut self, item: T);
}

/// A type that can build containers from items.
///
/// An implementation needs to absorb elements, and later reveal equivalent information
/// chunked into individual containers, but is free to change the data representation to
/// better fit the properties of the container.
///
/// Types implementing this trait should provide appropriate [`PushInto`] implementations such
/// that users can push the expected item types.
///
/// The owner extracts data in two ways. The opportunistic [`Self::extract`] method returns
/// any ready data, but doesn't need to produce partial outputs. In contrast, [`Self::finish`]
/// needs to produce all outputs, even partial ones. Caller should repeatedly call the functions
/// to drain pending or finished data.
///
/// The caller should consume the containers returned by [`Self::extract`] and
/// [`Self::finish`]. Implementations can recycle buffers, but should ensure that they clear
/// any remaining elements.
///
/// For example, a consolidating builder can aggregate differences in-place, but it has
/// to ensure that it preserves the intended information.
///
/// The trait does not prescribe any specific ordering guarantees, and each implementation can
/// decide to represent a push order for `extract` and `finish`, or not.
pub trait ContainerBuilder: Default + 'static {
    /// The container type we're building.
    type Container: Container;
    /// Extract assembled containers, potentially leaving unfinished data behind. Can
    /// be called repeatedly, for example while the caller can send data.
    ///
    /// Returns a `Some` if there is data ready to be shipped, and `None` otherwise.
    #[must_use]
    fn extract(&mut self) -> Option<&mut Self::Container>;
    /// Extract assembled containers and any unfinished data. Should
    /// be called repeatedly until it returns `None`.
    #[must_use]
    fn finish(&mut self) -> Option<&mut Self::Container>;
}

/// A default container builder that uses length and preferred capacity to chunk data.
///
/// Maintains a single empty allocation between [`Self::push_into`] and [`Self::extract`], but not
/// across [`Self::finish`] to maintain a low memory footprint.
///
/// Maintains FIFO order.
#[derive(Default, Debug)]
pub struct CapacityContainerBuilder<C>{
    /// Container that we're writing to.
    current: C,
    /// Emtpy allocation.
    empty: Option<C>,
    /// Completed containers pending to be sent.
    pending: VecDeque<C>,
}

impl<T, C: SizableContainer + PushInto<T>> PushInto<T> for CapacityContainerBuilder<C> {
    #[inline]
    fn push_into(&mut self, item: T) {
        if self.current.capacity() == 0 {
            self.current = self.empty.take().unwrap_or_default();
            // Discard any non-uniform capacity container.
            if self.current.capacity() != C::preferred_capacity() {
                self.current = C::default();
            }
            // Protect against non-emptied containers.
            self.current.clear();
        }
        // Ensure capacity
        if self.current.capacity() < C::preferred_capacity() {
            self.current.reserve(C::preferred_capacity() - self.current.len());
        }

        // Push item
        self.current.push(item);

        // Maybe flush
        if self.current.len() == self.current.capacity() {
            self.pending.push_back(std::mem::take(&mut self.current));
        }
    }
}

impl<C: Container> ContainerBuilder for CapacityContainerBuilder<C> {
    type Container = C;

    #[inline]
    fn extract(&mut self) -> Option<&mut C> {
        if let Some(container) = self.pending.pop_front() {
            self.empty = Some(container);
            self.empty.as_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut C> {
        if !self.current.is_empty() {
            self.pending.push_back(std::mem::take(&mut self.current));
        }
        self.empty = self.pending.pop_front();
        self.empty.as_mut()
    }
}

impl<C: Container> CapacityContainerBuilder<C> {
    /// Push a pre-formed container at this builder. This exists to maintain
    /// API compatibility.
    #[inline]
    pub fn push_container(&mut self, container: &mut C) {
        if !container.is_empty() {
            // Flush to maintain FIFO ordering.
            if self.current.len() > 0 {
                self.pending.push_back(std::mem::take(&mut self.current));
            }

            let mut empty = self.empty.take().unwrap_or_default();
            // Ideally, we'd discard non-uniformly sized containers, but we don't have
            // access to `len`/`capacity` of the container.
            empty.clear();

            self.pending.push_back(std::mem::replace(container, empty));
        }
    }
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

impl<T: Clone + 'static> SizableContainer for Vec<T> {
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

impl<T> PushInto<T> for Vec<T> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.push(item)
    }
}


impl<T: Clone> PushInto<&T> for Vec<T> {
    #[inline]
    fn push_into(&mut self, item: &T) {
        self.push(item.clone())
    }
}

impl<T: Clone> PushInto<&&T> for Vec<T> {
    #[inline]
    fn push_into(&mut self, item: &&T) {
        self.push_into(*item)
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
pub trait PushPartitioned: SizableContainer {
    /// Partition and push this container.
    ///
    /// Drain all elements from `self`, and use the function `index` to determine which `buffer` to
    /// append an element to. Call `flush` with an index and a buffer to send the data downstream.
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], index: I, flush: F)
    where
        for<'a> I: FnMut(&Self::Item<'a>) -> usize,
        F: FnMut(usize, &mut Self);
}

impl<C: SizableContainer> PushPartitioned for C where for<'a> C: PushInto<C::Item<'a>> {
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
        let size = std::mem::size_of::<T>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }
}
