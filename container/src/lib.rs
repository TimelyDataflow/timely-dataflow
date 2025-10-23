//! Specifications for containers

#![forbid(missing_docs)]

use std::collections::VecDeque;

/// A type containing a number of records accounted for by progress tracking.
///
/// The object stores a number of updates and thus is able to describe it count
/// (`update_count()`) and whether it is empty (`is_empty()`). It is empty if the
/// update count is zero.
pub trait Accountable {
    /// The number of records
    ///
    /// This number is used in progress tracking to confirm the receipt of some number
    /// of outstanding records, and it is highly load bearing. The main restriction is
    /// imposed on the `LengthPreservingContainerBuilder` trait, whose implementors
    /// must preserve the number of records.
    fn record_count(&self) -> i64;

    /// Determine if this contains any updates, corresponding to `update_count() == 0`.
    /// It is a correctness error for this to be anything other than `self.record_count() == 0`.
    #[inline] fn is_empty(&self) -> bool { self.record_count() == 0 }
}

/// A container that can drain itself.
///
/// Draining the container presents items in an implementation-specific order.
/// The container is in an undefined state after calling [`drain`]. Dropping
/// the iterator also leaves the container in an undefined state.
pub trait DrainContainer {
    /// The type of elements when draining the container.
    type Item<'a> where Self: 'a;
    /// Iterator type when draining the container.
    type DrainIter<'a>: Iterator<Item=Self::Item<'a>> where Self: 'a;
    /// Returns an iterator that drains the contents of this container.
    /// Drain leaves the container in an undefined state.
    fn drain(&mut self) -> Self::DrainIter<'_>;
}

/// A container that can be sized and reveals its capacity.
pub trait SizableContainer {
    /// Indicates that the container is "full" and should be shipped.
    fn at_capacity(&self) -> bool;
    /// Restores `self` to its desired capacity, if it has one.
    ///
    /// The `stash` argument is available, and may have the intended capacity.
    /// However, it may be non-empty, and may be of the wrong capacity. The
    /// method should guard against these cases.
    ///
    /// Assume that the `stash` is in an undefined state, and properly clear it
    /// before re-using it.
    fn ensure_capacity(&mut self, stash: &mut Option<Self>) where Self: Sized;
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
/// Implementations are allowed to re-use the contents of the mutable references left by the caller,
/// but they should ensure that they clear the contents before doing so.
///
/// For example, a consolidating builder can aggregate differences in-place, but it has
/// to ensure that it preserves the intended information.
///
/// The trait does not prescribe any specific ordering guarantees, and each implementation can
/// decide to represent a push order for `extract` and `finish`, or not.
pub trait ContainerBuilder: Default {
    /// The container type we're building.
    // The container is `Clone` because `Tee` requires it, otherwise we need to repeat it
    // all over Timely. `'static` because we don't want lifetimes everywhere.
    type Container;
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
    /// Indicates a good moment to release resources.
    ///
    /// By default, does nothing. Callers first needs to drain the contents using [`Self::finish`]
    /// before calling this function. The implementation should not change the contents of the
    /// builder.
    #[inline]
    fn relax(&mut self) { }
}

/// A wrapper trait indicating that the container building will preserve the number of records.
///
/// Specifically, the sum of record counts of all extracted and finished containers must equal the
/// number of accounted records that are pushed into the container builder.
/// If you have any questions about this trait you are best off not implementing it.
pub trait LengthPreservingContainerBuilder : ContainerBuilder { }

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
    /// Empty allocation.
    empty: Option<C>,
    /// Completed containers pending to be sent.
    pending: VecDeque<C>,
}

impl<T, C: SizableContainer + Default + PushInto<T>> PushInto<T> for CapacityContainerBuilder<C> {
    #[inline]
    fn push_into(&mut self, item: T) {
        // Ensure capacity
        self.current.ensure_capacity(&mut self.empty);

        // Push item
        self.current.push_into(item);

        // Maybe flush
        if self.current.at_capacity() {
            self.pending.push_back(std::mem::take(&mut self.current));
        }
    }
}

impl<C: Accountable + Default> ContainerBuilder for CapacityContainerBuilder<C> {
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

impl<C: Accountable + SizableContainer + Default> LengthPreservingContainerBuilder for CapacityContainerBuilder<C> { }

impl<T> Accountable for Vec<T> {
    #[inline] fn record_count(&self) -> i64 { i64::try_from(Vec::len(self)).unwrap() }
    #[inline] fn is_empty(&self) -> bool { Vec::is_empty(self) }
}

impl<T> DrainContainer for Vec<T> {
    type Item<'a> = T where T: 'a;
    type DrainIter<'a> = std::vec::Drain<'a, T> where Self: 'a;
    #[inline] fn drain(&mut self) -> Self::DrainIter<'_> {
        self.drain(..)
    }
}

impl<T> SizableContainer for Vec<T> {
    fn at_capacity(&self) -> bool {
        self.len() == self.capacity()
    }
    fn ensure_capacity(&mut self, stash: &mut Option<Self>) {
        if self.capacity() == 0 {
            *self = stash.take().unwrap_or_default();
            self.clear();
        }
        let preferred = buffer::default_capacity::<T>();
        if self.capacity() < preferred {
            self.reserve(preferred - self.capacity());
        }
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
    impl<T: crate::Accountable> crate::Accountable for std::rc::Rc<T> {
        #[inline] fn record_count(&self) -> i64 { self.as_ref().record_count() }
        #[inline] fn is_empty(&self) -> bool { self.as_ref().is_empty() }
    }
    impl<T> crate::DrainContainer for std::rc::Rc<T>
    where
        for<'a> &'a T: IntoIterator
    {
        type Item<'a> = <&'a T as IntoIterator>::Item where Self: 'a;
        type DrainIter<'a> = <&'a T as IntoIterator>::IntoIter where Self: 'a;
        #[inline] fn drain(&mut self) -> Self::DrainIter<'_> { self.into_iter() }
    }
}

mod arc {
    impl<T: crate::Accountable> crate::Accountable for std::sync::Arc<T> {
        #[inline] fn record_count(&self) -> i64 { self.as_ref().record_count() }
        #[inline] fn is_empty(&self) -> bool { self.as_ref().is_empty() }
    }
    impl<T> crate::DrainContainer for std::sync::Arc<T>
    where
        for<'a> &'a T: IntoIterator
    {
        type Item<'a> = <&'a T as IntoIterator>::Item where Self: 'a;
        type DrainIter<'a> = <&'a T as IntoIterator>::IntoIter where Self: 'a;
        #[inline] fn drain(&mut self) -> Self::DrainIter<'_> { self.into_iter() }
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
