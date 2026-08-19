//! The stamp of a message: the multiset of timestamps affixed to it.
//!
//! Each message in timely dataflow carries a stamp: a multiset of timestamps,
//! such that the message may only result in downstream work at times greater
//! or equal to some element. Like postage, the stamp records the capabilities
//! under which the message travels. Multiplicities are significant, as the
//! message is accounted once per element in progress tracking; the order of
//! elements is not, and is maintained sorted so that equal stamps are
//! structurally equal. The common case is a singleton stamp,
//! corresponding to the classic "one capability per message" design, and is
//! represented inline without allocation. Stamps may contain multiple elements
//! (e.g. a batch of updates stamped with its lower antichain, as in differential
//! dataflow) or no elements at all (data that makes no progress claims, and which
//! may be delivered after the frontier has passed all times in its payload).
//!
//! `Stamp` maintains its elements sorted by `Ord`, so that equal stamps are
//! structurally equal and stamps can be used as grouping and sorting keys.
//! Stamps built by insertion are minimal (an antichain), but stamps that have
//! crossed a scope boundary may contain comparable or duplicate elements: the
//! stamp is the multiset of pointstamps at which the message is accounted, and
//! boundary maps must preserve counts element-wise (see [`Stamp::map_pointwise`]).

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::order::PartialOrder;

/// A multiset of timestamps affixed to a message, stored sorted.
///
/// The elements are stored in increasing order under `Ord` so that `Eq`, `Ord`,
/// and `Hash` are structural. Stamps built by insertion are minimal antichains;
/// stamps mapped across scope boundaries may contain comparable or duplicate
/// elements, whose multiplicities are significant for progress accounting.
///
/// As with [`Antichain`](crate::progress::Antichain), the storage holds a single
/// element inline, so the overwhelmingly common zero- and one-element stamps
/// require no allocation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, columnar::Columnar)]
pub struct Stamp<T> {
    elements: SmallVec<[T; 1]>,
}

impl<T> Stamp<T> {
    /// An empty stamp, making no progress claims.
    pub fn new() -> Self {
        Stamp { elements: SmallVec::new() }
    }

    /// A stamp containing a single element.
    pub fn from_elem(element: T) -> Self {
        Stamp { elements: SmallVec::from_buf([element]) }
    }

    /// The elements of the stamp, in increasing `Ord` order.
    #[inline]
    pub fn elements(&self) -> &[T] {
        &self.elements[..]
    }

    /// An iterator over the elements of the stamp.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.elements.iter()
    }

    /// True iff the stamp contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// The number of elements in the stamp.
    #[inline]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// The elements of the stamp, by value, in increasing `Ord` order.
    #[inline]
    pub fn into_elements(self) -> Vec<T> {
        self.elements.into_vec()
    }

    /// The sole element of the stamp, if the stamp is a singleton.
    #[inline]
    pub fn as_singleton(&self) -> Option<&T> {
        if self.elements.len() == 1 { Some(&self.elements[0]) } else { None }
    }

    /// The sole element of the stamp; panics if the stamp is not a singleton.
    ///
    /// This supports pre-stamp interfaces that insist on a single timestamp per
    /// message; such interfaces cannot be used with multi- or zero-element stamps.
    #[inline]
    pub fn expect_singleton(&self) -> &T where T: std::fmt::Debug {
        self.as_singleton().unwrap_or_else(|| {
            panic!("expected a singleton stamp; found {:?} elements: {:?}", self.elements.len(), self.elements())
        })
    }
}

impl<T: PartialOrder + Ord> Stamp<T> {
    /// Inserts `element` unless it is redundant, removing elements it dominates.
    ///
    /// Returns true iff the element was inserted.
    pub fn insert(&mut self, element: T) -> bool {
        if self.elements.iter().any(|x| x.less_equal(&element)) {
            false
        } else {
            self.elements.retain(|x| !element.less_equal(x));
            let position = self.elements.partition_point(|x| x < &element);
            self.elements.insert(position, element);
            true
        }
    }

    /// True iff some element of the stamp is less or equal to `time`.
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_equal(time))
    }

    /// Maps each element through `logic`, discarding `None` results and
    /// restoring minimality and canonical order.
    ///
    /// This is the shape required when timestamps traverse path summaries
    /// (feedback edges), where elements may fail to traverse, or may become
    /// comparable after mapping. It may only be used by operators that account
    /// for their own consumed and produced messages, as it changes the number
    /// of pointstamps a message is accounted at.
    pub fn map_into<T2: PartialOrder + Ord>(&self, mut logic: impl FnMut(&T) -> Option<T2>) -> Stamp<T2> {
        let mut result = Stamp::new();
        for element in self.elements.iter() {
            if let Some(mapped) = logic(element) {
                result.insert(mapped);
            }
        }
        result
    }

    /// Maps each element through `logic`, preserving the number of elements and
    /// restoring sorted order, but *not* minimality.
    ///
    /// This is the shape required at scope boundaries (enter and leave), whose
    /// produced and consumed accounting is inferred independently at either end
    /// of the channel from the stamp itself: the counts must agree element-wise,
    /// so the map must not collapse elements that become comparable or equal
    /// after mapping (as when leaving a scope projects away a timestamp
    /// coordinate). The resulting stamp may contain comparable or duplicate
    /// elements; this weakens no guarantee, as the stamp promises only that
    /// message contents are greater or equal to *some* element.
    pub fn map_pointwise<T2: Ord>(&self, logic: impl FnMut(&T) -> T2) -> Stamp<T2> {
        let mut elements: SmallVec<[T2; 1]> = self.elements.iter().map(logic).collect();
        elements.sort();
        Stamp { elements }
    }
}

impl<T> Default for Stamp<T> {
    fn default() -> Self { Self::new() }
}

impl<T: PartialOrder + Ord> FromIterator<T> for Stamp<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut result = Stamp::new();
        for element in iter {
            result.insert(element);
        }
        result
    }
}

impl<'a, T> IntoIterator for &'a Stamp<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

impl<T: PartialOrder + Ord + Clone> From<crate::progress::Antichain<T>> for Stamp<T> {
    fn from(antichain: crate::progress::Antichain<T>) -> Self {
        let mut elements: SmallVec<[T; 1]> = antichain.into();
        elements.sort();
        Stamp { elements }
    }
}

impl<T: PartialOrder + Ord + Clone> From<Stamp<T>> for crate::progress::Antichain<T> {
    fn from(stamp: Stamp<T>) -> Self {
        stamp.into_elements().into()
    }
}
