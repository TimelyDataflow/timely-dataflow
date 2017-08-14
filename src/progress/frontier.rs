//! Tracks minimal sets of mutually incomparable elements of a partial order.

// use progress::CountMap;
use order::PartialOrder;

/// A set of mutually incomparable elements.
#[derive(Default, Clone, Debug)]
pub struct Antichain<T> {
    elements: Vec<T>
}

impl<T: PartialOrder> Antichain<T> {
    /// Updates the `Antichain` if the element is not greater than or equal to some present element.
    ///
    /// Returns true if element is added to the set
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

    /// Creates a new empty `Antichain`.
    pub fn new() -> Antichain<T> { Antichain { elements: Vec::new() } }

    /// Creates a new singleton `Antichain`.
    pub fn from_elem(element: T) -> Antichain<T> { Antichain { elements: vec![element] } }

    /// Clears the contents of the antichain.
    pub fn clear(&mut self) { self.elements.clear() }

    /// Returns true if any item in the antichain is strictly less than the argument.
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_than(time))
    }

    /// Returns true if any item in the antichain is less than or equal to the argument.
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_equal(time))
    }

    /// Returns true if every element of `other` is greater or equal to some element of `self`.
    #[inline]
    pub fn dominates(&self, other: &Antichain<T>) -> bool {
        other.elements().iter().all(|t2| self.elements().iter().any(|t1| t1.less_equal(t2)))
    }
    
    /// Reveals the elements in the antichain.
    #[inline] pub fn elements(&self) -> &[T] { &self.elements[..] }
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
/// orders two elements, the so does the `Ord` implementation).
///
/// The `MutableAntichain` implementation is done with the intent that updates to it are done in batches,
/// and it is acceptable to rebuild the frontier from scratch when a batch of updates change it. This means
/// that it can be expensive to maintain a large number of counts and change few elements near the frontier.
///
/// There is an `update_dirty` method for single updates that leave the `MutableAntichain` in a dirty state,
/// but I strongly recommend against using them unless you must (on part of timely progress tracking seems 
/// to be greatly simplified by access to this)
#[derive(Default, Debug, Clone)]
pub struct MutableAntichain<T: PartialOrder+Ord> {
    dirty: usize,
    updates: Vec<(T, i64)>,
    frontier: Vec<T>,
    frontier_temp: Vec<T>,
}

impl<T: PartialOrder+Ord+Clone+'static> MutableAntichain<T> {
    /// Creates a new empty `MutableAntichain`.
    #[inline]
    pub fn new() -> MutableAntichain<T> {
        MutableAntichain {
            dirty: 0,
            updates: Vec::new(),
            frontier:  Vec::new(),
            frontier_temp: Vec::new(),
        }
    }

    /// Removes all elements.
    #[inline]
    pub fn clear(&mut self) {
        self.dirty = 0;
        self.updates.clear();
        self.frontier.clear();
        self.frontier_temp.clear();
    }

    /// Reveals the minimal elements with positive count.
    #[inline]
    pub fn frontier(&self) -> &[T] { 
        assert!(self.dirty == 0);
        &self.frontier 
    }

    /// Creates a new singleton `MutableAntichain`.
    #[inline]
    pub fn new_bottom(bottom: T) -> MutableAntichain<T> {
        MutableAntichain {
            dirty: 0,
            updates: vec![(bottom.clone(), 1)],
            frontier: vec![bottom.clone()],
            frontier_temp: Vec::new(),
        }
    }

    /// Returns true if there are no elements in the `MutableAntichain`.
    #[inline]
    pub fn is_empty(&self) -> bool { 
        assert!(self.dirty == 0);        
        self.frontier.is_empty() 
    }

    /// Returns true if any item in the `MutableAntichain` is strictly less than the argument.
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        assert!(self.dirty == 0);
        self.frontier.iter().any(|x| x.less_than(time))
    }

    /// Returns true if any item in the `MutableAntichain` is less than or equal to the argument.
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        assert!(self.dirty == 0);
        self.frontier.iter().any(|x| x.less_equal(time))
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

    /// Applies updates to the antichain and applies `action` to each frontier change.
    ///
    /// This method applies a batch of updates and if any affects the frontier it is rebuilt.
    #[inline]
    pub fn update_iter<I>(&mut self, updates: I)
    where 
        I: IntoIterator<Item = (T, i64)>
    {
        self.update_iter_and(updates, |_,_| { });
    }

    /// Applies updates to the antichain and applies `action` to each frontier change.
    ///
    /// This method applies a batch of updates and if any affects the frontier it is rebuilt.
    #[inline]
    pub fn update_iter_and<I, A>(&mut self, updates: I, action: A)
    where 
        I: IntoIterator<Item = (T, i64)>,
        A: FnMut(&T, i64)
    {
        // track whether a rebuild is needed.
        let mut rebuild_required = false;

        for (time, delta) in updates {
            self.updates.push((time, delta));
            self.dirty += 1;
        }

        // determine if recently pushed data requires rebuilding the frontier.
        // note: this may be required even with an empty iterator, due to dirty data in self.updates.
        while self.dirty > 0 {

            let time = &self.updates[self.updates.len() - self.dirty].0;
            let delta = self.updates[self.updates.len() - self.dirty].1;

            let beyond_frontier = self.frontier.iter().any(|f| f.less_than(time));
            let before_frontier = !self.frontier.iter().any(|f| f.less_equal(time));
            rebuild_required = rebuild_required || !(beyond_frontier || (delta < 0 && before_frontier));
            
            self.dirty -= 1;
        }

        if rebuild_required {
            self.rebuild_and(action);
        }
    }

    /// Sorts and consolidates `self.updates` and applies `action` to any frontier changes.
    ///
    /// This method is meant to be used for bulk updates to the frontier, and does more work than one might do
    /// for single updates, but is meant to be an efficient way to process multiple updates together. This is 
    /// especially true when we want to apply very large numbers of updates.
    fn rebuild_and<A: FnMut(&T, i64)>(&mut self, mut action: A) {

        // sort and consolidate updates; retain non-zero accumulations.
        if self.updates.len() > 0 {
            self.updates.sort_by(|x,y| x.0.cmp(&y.0));
            for i in 0 .. self.updates.len() - 1 {
                if self.updates[i].0 == self.updates[i+1].0 {
                    self.updates[i+1].1 += self.updates[i].1;
                    self.updates[i].1 = 0;
                }
            }
            self.updates.retain(|x| x.1 != 0);
        }

        // build new frontier using strictly positive times.
        // as the times are sorted, we don't need to worry that we might displace frontier elements.
        for time in self.updates.iter().filter(|x| x.1 > 0) {
            if !self.frontier_temp.iter().any(|f| f.less_than(&time.0)) {
                self.frontier_temp.push(time.0.clone());
            }
        }

        // TODO: This is quadratic in the frontier size, but could be linear (with a merge).
        for time in self.frontier.iter() {
            if !self.frontier_temp.contains(time) {
                action(time, -1);
            }
        }
        ::std::mem::swap(&mut self.frontier, &mut self.frontier_temp);
        for time in self.frontier.iter() {
            if !self.frontier_temp.contains(time) {
                action(time, 1);
            }
        }
        self.frontier_temp.clear();
    }
}
