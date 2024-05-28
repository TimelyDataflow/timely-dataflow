//! A columnar container based on the columnation library.

use std::iter::FromIterator;

pub use columnation::*;

use crate::PushInto;

/// An append-only vector that store records as columns.
///
/// This container maintains elements that might conventionally own
/// memory allocations, but instead the pointers to those allocations
/// reference larger regions of memory shared with multiple instances
/// of the type. Elements can be retrieved as references, and care is
/// taken when this type is dropped to ensure that the correct memory
/// is returned (rather than the incorrect memory, from running the
/// elements `Drop` implementations).
pub struct TimelyStack<T: Columnation> {
    local: Vec<T>,
    inner: T::InnerRegion,
}

impl<T: Columnation> TimelyStack<T> {
    /// Construct a [TimelyStack], reserving space for `capacity` elements
    ///
    /// Note that the associated region is not initialized to a specific capacity
    /// because we can't generally know how much space would be required.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            local: Vec::with_capacity(capacity),
            inner: T::InnerRegion::default(),
        }
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_items<'a, I>(&mut self, items: I)
        where
            I: Iterator<Item= &'a T>+Clone,
            T: 'a,
    {
        self.local.reserve(items.clone().count());
        self.inner.reserve_items(items);
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item= &'a Self>+Clone,
    {
        self.local.reserve(regions.clone().map(|cs| cs.local.len()).sum());
        self.inner.reserve_regions(regions.map(|cs| &cs.inner));
    }



    /// Copies an element in to the region.
    ///
    /// The element can be read by indexing
    pub fn copy(&mut self, item: &T) {
        // TODO: Some types `T` should just be cloned.
        // E.g. types that are `Copy` or vecs of ZSTs.
        unsafe {
            self.local.push(self.inner.copy(item));
        }
    }
    /// Empties the collection.
    pub fn clear(&mut self) {
        unsafe {
            // Unsafety justified in that setting the length to zero exposes
            // no invalid data.
            self.local.set_len(0);
            self.inner.clear();
        }
    }
    /// Retain elements that pass a predicate, from a specified offset.
    ///
    /// This method may or may not reclaim memory in the inner region.
    pub fn retain_from<P: FnMut(&T) -> bool>(&mut self, index: usize, mut predicate: P) {
        let mut write_position = index;
        for position in index..self.local.len() {
            if predicate(&self[position]) {
                // TODO: compact the inner region and update pointers.
                self.local.swap(position, write_position);
                write_position += 1;
            }
        }
        unsafe {
            // Unsafety justified in that `write_position` is no greater than
            // `self.local.len()` and so this exposes no invalid data.
            self.local.set_len(write_position);
        }
    }

    /// Unsafe access to `local` data. The slices stores data that is backed by a region
    /// allocation. Therefore, it is undefined behavior to mutate elements of the `local` slice.
    ///
    /// # Safety
    /// Elements within `local` can be reordered, but not mutated, removed and/or dropped.
    pub unsafe fn local(&mut self) -> &mut [T] {
        &mut self.local[..]
    }

    /// Estimate the memory capacity in bytes.
    #[inline]
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        let size_of = std::mem::size_of::<T>();
        callback(self.local.len() * size_of, self.local.capacity() * size_of);
        self.inner.heap_size(callback);
    }

    /// Estimate the consumed memory capacity in bytes, summing both used and total capacity.
    #[inline]
    pub fn summed_heap_size(&self) -> (usize, usize) {
        let (mut length, mut capacity) = (0, 0);
        self.heap_size(|len, cap| {
            length += len;
            capacity += cap
        });
        (length, capacity)
    }

    /// The length in items.
    #[inline]
    pub fn len(&self) -> usize {
        self.local.len()
    }

    /// The capacity of the local vector.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.local.capacity()
    }

    /// Reserve space for `additional` elements.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.local.reserve(additional)
    }
}

impl<A: Columnation, B: Columnation> TimelyStack<(A, B)> {
    /// Copies a destructured tuple `(A, B)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but not
    /// not all elements are available as owned data.
    ///
    /// The element can be read by indexing
    pub fn copy_destructured(&mut self, t1: &A, t2: &B) {
        unsafe {
            self.local.push(self.inner.copy_destructured(t1, t2));
        }
    }
}

impl<A: Columnation, B: Columnation, C: Columnation> TimelyStack<(A, B, C)> {
    /// Copies a destructured tuple `(A, B, C)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but not
    /// not all elements are available as owned data.
    ///
    /// The element can be read by indexing
    pub fn copy_destructured(&mut self, r0: &A, r1: &B, r2: &C) {
        unsafe {
            self.local.push(self.inner.copy_destructured(r0, r1, r2));
        }
    }
}

impl<T: Columnation> std::ops::Deref for TimelyStack<T> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.local[..]
    }
}

impl<T: Columnation> Drop for TimelyStack<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T: Columnation> Default for TimelyStack<T> {
    fn default() -> Self {
        Self {
            local: Vec::new(),
            inner: T::InnerRegion::default(),
        }
    }
}

impl<'a, A: 'a + Columnation> FromIterator<&'a A> for TimelyStack<A> {
    fn from_iter<T: IntoIterator<Item = &'a A>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let mut c = TimelyStack::<A>::with_capacity(iter.size_hint().0);
        while let Some(element) = iter.next() {
            c.copy(element);
        }

        c
    }
}

impl<T: Columnation + PartialEq> PartialEq for TimelyStack<T> {
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&self[..], &other[..])
    }
}

impl<T: Columnation + Eq> Eq for TimelyStack<T> {}

impl<T: Columnation + std::fmt::Debug> std::fmt::Debug for TimelyStack<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self[..].fmt(f)
    }
}

impl<T: Columnation> Clone for TimelyStack<T> {
    fn clone(&self) -> Self {
        let mut new: Self = Default::default();
        for item in &self[..] {
            new.copy(item);
        }
        new
    }

    fn clone_from(&mut self, source: &Self) {
        self.clear();
        for item in &source[..] {
            self.copy(item);
        }
    }
}

impl<T: Columnation> PushInto<T> for TimelyStack<T> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.copy(&item);
    }
}

impl<T: Columnation> PushInto<&T> for TimelyStack<T> {
    #[inline]
    fn push_into(&mut self, item: &T) {
        self.copy(item);
    }
}


impl<T: Columnation> PushInto<&&T> for TimelyStack<T> {
    #[inline]
    fn push_into(&mut self, item: &&T) {
        self.copy(*item);
    }
}

mod serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::columnation::{Columnation, TimelyStack};

    impl<T: Columnation + Serialize> Serialize for TimelyStack<T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            use serde::ser::SerializeSeq;
            let mut seq = serializer.serialize_seq(Some(self.local.len()))?;
            for element in &self[..] {
                seq.serialize_element(element)?;
            }
            seq.end()
        }
    }

    impl<'a, T: Columnation + Deserialize<'a>> Deserialize<'a> for TimelyStack<T> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'a>,
        {
            use serde::de::{SeqAccess, Visitor};
            use std::fmt;
            use std::marker::PhantomData;
            struct TimelyStackVisitor<T> {
                marker: PhantomData<T>,
            }

            impl<'de, T: Columnation> Visitor<'de> for TimelyStackVisitor<T>
            where
                T: Deserialize<'de>,
            {
                type Value = TimelyStack<T>;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a sequence")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    let local = Vec::with_capacity(
                        seq.size_hint()
                            .unwrap_or(crate::buffer::default_capacity::<T>()),
                    );
                    let mut stack = TimelyStack {
                        local,
                        inner: T::InnerRegion::default(),
                    };

                    while let Some(value) = seq.next_element()? {
                        stack.copy(&value);
                    }

                    Ok(stack)
                }
            }

            let visitor = TimelyStackVisitor {
                marker: PhantomData,
            };
            deserializer.deserialize_seq(visitor)
        }
    }
}

mod container {
    use std::ops::Deref;
    use crate::{Container, SizableContainer};

    use crate::columnation::{Columnation, TimelyStack};

    impl<T: Columnation + 'static> Container for TimelyStack<T> {
        type ItemRef<'a> = &'a T where Self: 'a;
        type Item<'a> = &'a T where Self: 'a;

        fn len(&self) -> usize {
            self.local.len()
        }

        fn is_empty(&self) -> bool {
            self.local.is_empty()
        }

        fn clear(&mut self) {
            TimelyStack::clear(self)
        }

        type Iter<'a> = std::slice::Iter<'a, T>;

        fn iter(&self) -> Self::Iter<'_> {
            self.deref().iter()
        }

        type DrainIter<'a> = std::slice::Iter<'a, T>;

        fn drain(&mut self) -> Self::DrainIter<'_> {
            (*self).iter()
        }
    }

    impl<T: Columnation + 'static> SizableContainer for TimelyStack<T> {
        fn capacity(&self) -> usize {
            self.capacity()
        }

        fn preferred_capacity() -> usize {
            crate::buffer::default_capacity::<T>()
        }

        fn reserve(&mut self, additional: usize) {
            self.reserve(additional)
        }
    }
}

mod flatcontainer {
    //! A bare-bones flatcontainer region implementation for [`TimelyStack`].

    use columnation::Columnation;
    use flatcontainer::{Push, Region, ReserveItems};
    use crate::columnation::TimelyStack;

    #[derive(Debug, Clone)]
    struct ColumnationRegion<T: Columnation> {
        inner: TimelyStack<T>,
    }

    impl<T: Columnation> Default for ColumnationRegion<T> {
        fn default() -> Self {
            Self { inner: Default::default() }
        }
    }

    impl<T: Columnation + Clone> Region for ColumnationRegion<T> {
        type Owned = T;
        type ReadItem<'a> = &'a T where Self: 'a;
        type Index = usize;

        fn merge_regions<'a>(regions: impl Iterator<Item=&'a Self> + Clone) -> Self where Self: 'a {
            let mut inner = TimelyStack::default();
            inner.reserve_regions(regions.map(|r| &r.inner));
            Self { inner}
        }

        fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
            &self.inner[index]
        }

        fn reserve_regions<'a, I>(&mut self, regions: I) where Self: 'a, I: Iterator<Item=&'a Self> + Clone {
            self.inner.reserve_regions(regions.map(|r| &r.inner));
        }

        fn clear(&mut self) {
            self.inner.clear();
        }

        fn heap_size<F: FnMut(usize, usize)>(&self, callback: F) {
            self.inner.heap_size(callback);
        }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> where Self: 'a {
            item
        }
    }

    impl<T: Columnation + Clone> Push<T> for ColumnationRegion<T> {
        fn push(&mut self, item: T) -> Self::Index {
            self.inner.copy(&item);
            self.inner.len() - 1
        }
    }

    impl<T: Columnation + Clone> Push<&T> for ColumnationRegion<T> {
        fn push(&mut self, item: &T) -> Self::Index {
            self.inner.copy(item);
            self.inner.len() - 1
        }
    }

    impl<T: Columnation + Clone> Push<&&T> for ColumnationRegion<T> {
        fn push(&mut self, item: &&T) -> Self::Index {
            self.inner.copy(*item);
            self.inner.len() - 1
        }
    }

    impl<'a, T: Columnation + Clone + 'a> ReserveItems<&'a T> for ColumnationRegion<T> {
        fn reserve_items<I>(&mut self, items: I) where I: Iterator<Item=&'a T> + Clone {
            self.inner.reserve_items(items);
        }
    }
}
