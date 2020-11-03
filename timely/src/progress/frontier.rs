//! Tracks minimal sets of mutually incomparable elements of a partial order.

use crate::progress::ChangeBatch;
use crate::order::PartialOrder;

/// A set of mutually incomparable elements.
///
/// An antichain is a set of partially ordered elements, each of which is incomparable to the others.
/// This antichain implementation allows you to repeatedly introduce elements to the antichain, and
/// which will evict larger elements to maintain the *minimal* antichain, those incomparable elements
/// no greater than any other element.
///
/// Two antichains are equal if the contain the same set of elements, even if in different orders.
/// This can make equality testing quadratic, though linear in the common case that the sequences
/// are identical.
#[derive(Clone, Debug, Default, Abomonation, Serialize, Deserialize)]
pub struct Antichain<T> {
    elements: Vec<T>
}

impl<T: PartialOrder> Antichain<T> {
    /// Updates the `Antichain` if the element is not greater than or equal to some present element.
    ///
    /// Returns true if element is added to the set
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::new();
    /// assert!(frontier.insert(2));
    /// assert!(!frontier.insert(3));
    ///```
    pub fn insert(&mut self, element: T) -> bool {
        if !self.elements.iter().any(|x| x.less_equal(&element)) {
            self.elements.retain(|x| !element.less_equal(x));
            self.elements.push(element);
            true
        }
        else {
            false
        }
    }

    /// Performs a sequence of insertion and return true iff any insertion does.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::new();
    /// assert!(frontier.extend(Some(3)));
    /// assert!(frontier.extend(vec![2, 5]));
    /// assert!(!frontier.extend(vec![3, 4]));
    ///```
    pub fn extend<I: IntoIterator<Item=T>>(&mut self, iterator: I) -> bool {
        let mut added = false;
        for element in iterator {
            added = self.insert(element) || added;
        }
        added
    }

    /// Returns true if any item in the antichain is strictly less than the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert!(frontier.less_than(&3));
    /// assert!(!frontier.less_than(&2));
    /// assert!(!frontier.less_than(&1));
    ///
    /// frontier.clear();
    /// assert!(!frontier.less_than(&3));
    ///```
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_than(time))
    }

    /// Returns true if any item in the antichain is less than or equal to the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert!(frontier.less_equal(&3));
    /// assert!(frontier.less_equal(&2));
    /// assert!(!frontier.less_equal(&1));
    ///
    /// frontier.clear();
    /// assert!(!frontier.less_equal(&3));
    ///```
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_equal(time))
    }

    /// Returns true if every element of `other` is greater or equal to some element of `self`.
    #[deprecated(since="0.12.0", note="please use `PartialOrder::less_equal` instead")]
    #[inline]
    pub fn dominates(&self, other: &Antichain<T>) -> bool {
        <Self as PartialOrder>::less_equal(self, other)
    }
}

impl<T> Antichain<T> {

    /// Creates a new empty `Antichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::<u32>::new();
    ///```
    pub fn new() -> Antichain<T> { Antichain { elements: Vec::new() } }

    /// Creates a new singleton `Antichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    ///```
    pub fn from_elem(element: T) -> Antichain<T> { Antichain { elements: vec![element] } }

    /// Clears the contents of the antichain.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// frontier.clear();
    /// assert!(frontier.elements().is_empty());
    ///```
    pub fn clear(&mut self) { self.elements.clear() }

    /// Sorts the elements so that comparisons between antichains can be made.
    pub fn sort(&mut self) where T: Ord { self.elements.sort() }

    /// Reveals the elements in the antichain.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert_eq!(frontier.elements(), &[2]);
    ///```
    #[inline] pub fn elements(&self) -> &[T] { &self.elements[..] }

    /// Reveals the elements in the antichain.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert_eq!(&*frontier.borrow(), &[2]);
    ///```
    #[inline] pub fn borrow(&self) -> AntichainRef<T> { AntichainRef::new(&self.elements[..]) }}

impl<T: PartialEq> PartialEq for Antichain<T> {
    fn eq(&self, other: &Self) -> bool {
        // Lengths should be the same, with the option for fast acceptance if identical.
        self.elements().len() == other.elements().len() &&
        (
            self.elements().iter().zip(other.elements().iter()).all(|(t1,t2)| t1 == t2) ||
            self.elements().iter().all(|t1| other.elements().iter().any(|t2| t1.eq(t2)))
        )
    }
}

impl<T: Eq> Eq for Antichain<T> { }

impl<T: PartialOrder> PartialOrder for Antichain<T> {
    fn less_equal(&self, other: &Self) -> bool {
        other.elements().iter().all(|t2| self.elements().iter().any(|t1| t1.less_equal(t2)))
    }
}

impl<T: PartialOrder> From<Vec<T>> for Antichain<T> {
    fn from(vec: Vec<T>) -> Self {
        // TODO: We could re-use `vec` with some care.
        let mut temp = Antichain::new();
        for elem in vec.into_iter() { temp.insert(elem); }
        temp
    }
}

/// An antichain based on a multiset whose elements frequencies can be updated.
///
/// The `MutableAntichain` maintains frequencies for many elements of type `T`, and exposes the set
/// of elements with positive count not greater than any other elements with positive count. The
/// antichain may both advance and retreat; the changes do not all need to be to elements greater or
/// equal to some elements of the frontier.
///
/// The type `T` must implement `PartialOrder` as well as `Ord`. The implementation of the `Ord` trait
/// is used to efficiently organize the updates for cancellation, and to efficiently determine the lower
/// bounds, and only needs to not contradict the `PartialOrder` implementation (that is, if `PartialOrder`
/// orders two elements, then so does the `Ord` implementation).
///
/// The `MutableAntichain` implementation is done with the intent that updates to it are done in batches,
/// and it is acceptable to rebuild the frontier from scratch when a batch of updates change it. This means
/// that it can be expensive to maintain a large number of counts and change few elements near the frontier.
///
/// There is an `update_dirty` method for single updates that leave the `MutableAntichain` in a dirty state,
/// but I strongly recommend against using them unless you must (on part of timely progress tracking seems
/// to be greatly simplified by access to this)
#[derive(Clone, Debug, Abomonation, Serialize, Deserialize)]
pub struct MutableAntichain<T: PartialOrder+Ord> {
    dirty: usize,
    updates: Vec<(T, i64)>,
    frontier: Vec<T>,
    changes: ChangeBatch<T>,
}

impl<T: PartialOrder+Ord+Clone> MutableAntichain<T> {
    /// Creates a new empty `MutableAntichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let frontier = MutableAntichain::<usize>::new();
    /// assert!(frontier.is_empty());
    ///```
    #[inline]
    pub fn new() -> MutableAntichain<T> {
        MutableAntichain {
            dirty: 0,
            updates: Vec::new(),
            frontier:  Vec::new(),
            changes: ChangeBatch::new(),
        }
    }

    /// Removes all elements.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::<usize>::new();
    /// frontier.clear();
    /// assert!(frontier.is_empty());
    ///```
    #[inline]
    pub fn clear(&mut self) {
        self.dirty = 0;
        self.updates.clear();
        self.frontier.clear();
        self.changes.clear();
    }

    /// This method deletes the contents. Unlike `clear` it records doing so.
    pub fn empty(&mut self) {
        for index in 0 .. self.updates.len() { self.updates[index].1 = 0; }
        self.dirty = self.updates.len();
    }

    /// Reveals the minimal elements with positive count.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::<usize>::new();
    /// assert!(frontier.frontier().len() == 0);
    ///```
    #[inline]
    pub fn frontier(&self) -> AntichainRef<T> {
        debug_assert_eq!(self.dirty, 0);
        AntichainRef::new(&self.frontier)
    }

    /// Creates a new singleton `MutableAntichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::{AntichainRef, MutableAntichain};
    ///
    /// let mut frontier = MutableAntichain::new_bottom(0u64);
    /// assert!(frontier.frontier() == AntichainRef::new(&[0u64]));
    ///```
    #[inline]
    pub fn new_bottom(bottom: T) -> MutableAntichain<T> {
        MutableAntichain {
            dirty: 0,
            updates: vec![(bottom.clone(), 1)],
            frontier: vec![bottom],
            changes: ChangeBatch::new(),
        }
    }

    /// Returns true if there are no elements in the `MutableAntichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::<usize>::new();
    /// assert!(frontier.is_empty());
    ///```
    #[inline]
    pub fn is_empty(&self) -> bool {
        debug_assert_eq!(self.dirty, 0);
        self.frontier.is_empty()
    }

    /// Returns true if any item in the `MutableAntichain` is strictly less than the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// assert!(!frontier.less_than(&0));
    /// assert!(!frontier.less_than(&1));
    /// assert!(frontier.less_than(&2));
    ///```
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        debug_assert_eq!(self.dirty, 0);
        self.frontier().less_than(time)
    }

    /// Returns true if any item in the `MutableAntichain` is less than or equal to the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// assert!(!frontier.less_equal(&0));
    /// assert!(frontier.less_equal(&1));
    /// assert!(frontier.less_equal(&2));
    ///```
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        debug_assert_eq!(self.dirty, 0);
        self.frontier().less_equal(time)
    }

    /// Allows a single-element push, but dirties the antichain and prevents inspection until cleaned.
    ///
    /// At the moment inspection is prevented via panic, so best be careful (this should probably be fixed).
    /// It is *very* important if you want to use this method that very soon afterwards you call something
    /// akin to `update_iter`, perhaps with a `None` argument if you have no more data, as this method will
    /// tidy up the internal representation.
    #[inline]
    pub fn update_dirty(&mut self, time: T, delta: i64) {
        self.updates.push((time, delta));
        self.dirty += 1;
    }

    /// Applies updates to the antichain and enumerates any changes.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::{AntichainRef, MutableAntichain};
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// let changes =
    /// frontier
    ///     .update_iter(vec![(1, -1), (2, 7)])
    ///     .collect::<Vec<_>>();
    ///
    /// assert!(frontier.frontier() == AntichainRef::new(&[2]));
    /// assert!(changes == vec![(1, -1), (2, 1)]);
    ///```
    #[inline]
    pub fn update_iter<'a, I>(&'a mut self, updates: I) -> ::std::vec::Drain<'a, (T, i64)>
    where
        I: IntoIterator<Item = (T, i64)>,
    {
        for (time, delta) in updates {
            self.updates.push((time, delta));
            self.dirty += 1;
        }

        // track whether a rebuild is needed.
        let mut rebuild_required = false;

        // determine if recently pushed data requires rebuilding the frontier.
        // note: this may be required even with an empty iterator, due to dirty data in self.updates.
        while self.dirty > 0 && !rebuild_required {

            let time = &self.updates[self.updates.len() - self.dirty].0;
            let delta = self.updates[self.updates.len() - self.dirty].1;

            let beyond_frontier = self.frontier.iter().any(|f| f.less_than(time));
            let before_frontier = !self.frontier.iter().any(|f| f.less_equal(time));
            rebuild_required = rebuild_required || !(beyond_frontier || (delta < 0 && before_frontier));

            self.dirty -= 1;
        }
        self.dirty = 0;

        if rebuild_required {
            self.rebuild()
        }
        self.changes.drain()
    }

    /// Sorts and consolidates `self.updates` and applies `action` to any frontier changes.
    ///
    /// This method is meant to be used for bulk updates to the frontier, and does more work than one might do
    /// for single updates, but is meant to be an efficient way to process multiple updates together. This is
    /// especially true when we want to apply very large numbers of updates.
    fn rebuild(&mut self) {

        // sort and consolidate updates; retain non-zero accumulations.
        if !self.updates.is_empty() {
            self.updates.sort_by(|x,y| x.0.cmp(&y.0));
            for i in 0 .. self.updates.len() - 1 {
                if self.updates[i].0 == self.updates[i+1].0 {
                    self.updates[i+1].1 += self.updates[i].1;
                    self.updates[i].1 = 0;
                }
            }
            self.updates.retain(|x| x.1 != 0);
        }

        for time in self.frontier.drain(..) {
            self.changes.update(time, -1);
        }

        // build new frontier using strictly positive times.
        // as the times are sorted, we don't need to worry that we might displace frontier elements.
        for time in self.updates.iter().filter(|x| x.1 > 0) {
            if !self.frontier.iter().any(|f| f.less_equal(&time.0)) {
                self.frontier.push(time.0.clone());
            }
        }

        for time in self.frontier.iter() {
            self.changes.update(time.clone(), 1);
        }
    }

    /// Reports the count for a queried time.
    pub fn count_for(&self, query_time: &T) -> i64 {
        self.updates
            .iter()
            .filter(|td| td.0.eq(query_time))
            .map(|td| td.1)
            .sum()
    }
}

/// Extension trait for filtering time changes through antichains.
pub trait MutableAntichainFilter<T: PartialOrder+Ord+Clone> {
    /// Filters time changes through an antichain.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::{MutableAntichain, MutableAntichainFilter};
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// let changes =
    /// vec![(1, -1), (2, 7)]
    ///     .filter_through(&mut frontier)
    ///     .collect::<Vec<_>>();
    ///
    /// assert!(changes == vec![(1, -1), (2, 1)]);
    ///```
    fn filter_through(self, antichain: &mut MutableAntichain<T>) -> ::std::vec::Drain<(T,i64)>;
}

impl<T: PartialOrder+Ord+Clone, I: IntoIterator<Item=(T,i64)>> MutableAntichainFilter<T> for I {
    fn filter_through(self, antichain: &mut MutableAntichain<T>) -> ::std::vec::Drain<(T,i64)> {
        antichain.update_iter(self.into_iter())
    }
}

/// A wrapper for elements of an antichain.
#[derive(Debug)]
pub struct AntichainRef<'a, T: 'a> {
    /// Elements contained in the antichain.
    frontier: &'a [T],
}

impl<'a, T: 'a> Clone for AntichainRef<'a, T> {
    fn clone(&self) -> Self {
        Self {
            frontier: self.frontier.clone(),
        }
    }
}
impl<'a, T: 'a> Copy for AntichainRef<'a, T> { }

impl<'a, T: 'a> AntichainRef<'a, T> {
    /// Create a new `AntichainRef` from a reference to a slice of elements forming the frontier.
    ///
    /// This method does not check that this antichain has any particular properties, for example
    /// that there are no elements strictly less than other elements.
    pub fn new(frontier: &'a [T]) -> Self {
        Self {
            frontier,
        }
    }

    /// Constructs an owned antichain from the antichain reference.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::{Antichain, frontier::AntichainRef};
    ///
    /// let frontier = AntichainRef::new(&[1u64]);
    /// assert_eq!(frontier.to_owned(), Antichain::from_elem(1u64));
    ///```
    pub fn to_owned(&self) -> Antichain<T> where T: Clone {
        Antichain {
            elements: self.frontier.to_vec()
        }
    }
}

impl<'a, T: 'a+PartialOrder> AntichainRef<'a, T> {

    /// Returns true if any item in the `AntichainRef` is strictly less than the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::AntichainRef;
    ///
    /// let frontier = AntichainRef::new(&[1u64]);
    /// assert!(!frontier.less_than(&0));
    /// assert!(!frontier.less_than(&1));
    /// assert!(frontier.less_than(&2));
    ///```
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.iter().any(|x| x.less_than(time))
    }

    /// Returns true if any item in the `AntichainRef` is less than or equal to the argument.
    #[inline]
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::AntichainRef;
    ///
    /// let frontier = AntichainRef::new(&[1u64]);
    /// assert!(!frontier.less_equal(&0));
    /// assert!(frontier.less_equal(&1));
    /// assert!(frontier.less_equal(&2));
    ///```
    pub fn less_equal(&self, time: &T) -> bool {
        self.iter().any(|x| x.less_equal(time))
    }
}

impl<'a, T: PartialEq> PartialEq for AntichainRef<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        // Lengths should be the same, with the option for fast acceptance if identical.
        self.len() == other.len() &&
        (
            self.iter().zip(other.iter()).all(|(t1,t2)| t1 == t2) ||
            self.iter().all(|t1| other.iter().any(|t2| t1.eq(t2)))
        )
    }
}

impl<'a, T: Eq> Eq for AntichainRef<'a, T> { }

impl<'a, T: PartialOrder> PartialOrder for AntichainRef<'a, T> {
    fn less_equal(&self, other: &Self) -> bool {
        other.iter().all(|t2| self.iter().any(|t1| t1.less_equal(t2)))
    }
}

impl<'a, T> ::std::ops::Deref for AntichainRef<'a, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.frontier
    }
}

impl<'a, T: 'a> ::std::iter::IntoIterator for &'a AntichainRef<'a, T> {
    type Item = &'a T;
    type IntoIter = ::std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
