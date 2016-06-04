//! Tracks minimal sets of mutually incomparable elements of a partial order.

use std::fmt::Debug;
use std::default::Default;
use std::cmp::Ordering;

use progress::CountMap;

/// A set of mutually incomparable elements.
#[derive(Default, Clone, Debug)]
pub struct Antichain<T> {
    elements: Vec<T>
}

impl<T: PartialOrd+Eq+Copy+Debug> Antichain<T> {
    /// Updates the `Antichain` if the element is not greater than or equal to some present element.
    ///
    /// Returns true if element is added to the set
    pub fn insert(&mut self, element: T) -> bool {
        // bail if any element exceeds the candidate
        if self.elements.iter().any(|x| element.ge(x)) {
            return false;
        }
        let mut removed = 0;
        for index in 0..self.elements.len() {
            let new_index = index - removed;
            if element.lt(&self.elements[new_index]) {
                self.elements.swap_remove(new_index);
                removed += 1;
            }
        }

        self.elements.push(element);
        true
    }

    /// Creates a new empty `Antichain`.
    pub fn new() -> Antichain<T> { Antichain { elements: Vec::new() } }

    /// Creates a new singleton `Antichain`.
    pub fn from_elem(element: T) -> Antichain<T> { Antichain { elements: vec![element] } }

    /// Reveals the elements in the `Antichain`.
    pub fn elements(&self) -> &[T] { &self.elements[..] }
}

/// An antichain based on a multiset whose elements frequencies can be updated.
///
/// In particular, unlike `Antichain`, element frequencies can be decremented, removing elements in
/// the antichain and revealing other elements that were previously obscured.
/// As part of this, the `MutableAntichain` must maintain the frequences for all elements, not just
/// those in the antichain at the moment.
#[derive(Default, Debug, Clone)]
pub struct MutableAntichain<T:Eq> {
    occurrences:    CountMap<T>,    // occurrence count of each time
    precedents:     Vec<(T, i64)>,  // counts number of distinct times in occurences strictly less than element
    elements:       Vec<T>,         // the set of times with precedent count == 0
}

impl<T: PartialOrd+Eq+Clone+Debug+'static> MutableAntichain<T> {
    /// Creates a new empty `MutableAntichain`.
    pub fn new() -> MutableAntichain<T> {
        MutableAntichain {
            occurrences:    Default::default(),
            precedents:     Default::default(),
            elements:       Vec::new(),
        }
    }

    /// Removes all elements from the antichain.
    pub fn clear(&mut self) {
        self.occurrences.clear();
        self.precedents.clear();
        self.elements.clear();
    }

    /// Reveals the element in the `MutableAntichain`.
    pub fn elements(&self) -> &[T] { &self.elements }

    /// Creates a new singleton `MutableAntichain`.
    pub fn new_bottom(bottom: T) -> MutableAntichain<T> {
        let mut result = MutableAntichain::new();
        result.update_weight(&bottom, 1, &mut Default::default());
        result
    }

    /// Returns true if there are no elements in the `MutableAntichain`.
    pub fn empty(&self) -> bool { self.elements.is_empty() }

    /// Returns true if any item in the `MutableAntichain` is strictly less than the argument.
    #[inline]
    pub fn lt(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.lt(time))
    }

    /// Returns true if any item in the `MutableAntichain` is less than or equal to the argument.
    #[inline]
    pub fn le(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.le(time))
    }

    /// Returns the number of times an element exists in the set.
    #[inline]
    pub fn count(&self, time: &T) -> Option<i64> {
        self.occurrences.iter().filter(|x| time.eq(&x.0)).next().map(|i| i.1)
    }

    // TODO : Four different versions of basically the same code. Fix that!
    // TODO : Should this drain updates through to the CM? Check out uses!
    /// Incorporates `updates` into the antichain, pushing frontier changes into `results`.
    pub fn update_into_cm(&mut self, updates: &CountMap<T>, results: &mut CountMap<T>) -> () {
        self.update_iter_and(updates.iter().cloned(), |time, val| { results.update(time, val); });
    }

    /// Performs a single update to the antichain, pushing frontier changes into `results`.
    pub fn update_weight(&mut self, elem: &T, delta: i64, results: &mut CountMap<T>) -> () {
        self.update_and(elem, delta, |time, delta| { results.update(time, delta); });
    }

    /// Applies a single update to the antichain.
    #[inline] pub fn update(&mut self, elem: &T, delta: i64) { self.update_and(elem, delta, |_,_| {}); }

    /// Applies updates to the antichain and applies `action` to each frontier change. 
    //#[inline(always)]
    pub fn update_iter_and<I: Iterator<Item = (T, i64)>,
                           A: FnMut(&T, i64) -> ()>(&mut self, updates: I, mut action: A) -> () {
        for (ref elem, delta) in updates {
            self.update_and(elem, delta, |t,d| action(t,d));
        }
    }

    /// Tests the size of the antichain against a threshould.
    ///
    /// This is a diagnostic method in place to observe when antichains become surprisingly large.
    /// If either `self.occurrences` or `self.precedents` exceeds the threshold, they are listed.
    #[inline]
    pub fn test_size(&self, threshold: usize, name: &str) {
        if self.occurrences.len() > threshold {
            println!("{}:\toccurrences:\tlen() = {}", name, self.occurrences.len());
            // for &(ref key, val) in self.occurrences.elements().iter() { println!("{}: \toccurrence: {:?} : {:?}", name, key, val); }
            // panic!();
        }
        if self.precedents.len() > threshold {
            println!("{}: precedents:\tlen() = {}", name, self.precedents.len());
        }
    }

    /// Applies an update to the antichain and takes an action on any frontier changes.
    #[inline]
    pub fn update_and<A: FnMut(&T, i64)->()>(&mut self, elem: &T, delta: i64, mut action: A) -> () {

        if delta != 0 {
            // self.test_size(100, "???");
            let new_value = self.occurrences.update(elem, delta);
            let old_value = new_value - delta;

            // if the value went from non-positive to positive we need to update self.precedents
            if old_value <= 0 && new_value > 0 {
                let mut preceded_by = 0;

                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in &mut self.precedents {
                    if let Some(comparison) = elem.partial_cmp(key) {
                        match comparison {
                            Ordering::Less    => {
                                if *val == 0 {
                                    self.elements.retain(|x| x != key);
                                    action(key, -1);
                                }
                                *val += 1;
                            },
                            Ordering::Equal   => { panic!("surprising!"); },
                            Ordering::Greater => { preceded_by += 1; },
                        }
                    }
                }

                // insert count always; maybe put in elements
                self.precedents.push((elem.clone(), preceded_by));
                if preceded_by == 0 {
                    self.elements.push(elem.clone());
                    action(elem, 1);
                }
            }

            // if the value went from positive to non-positive we need to update self.precedents.
            if old_value > 0 && new_value <= 0 {
                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in &mut self.precedents {
                    if elem < key {
                        *val -= 1;
                        if *val == 0 {
                            self.elements.push(key.clone());
                            action(key, 1);
                        }
                    }
                }

                if let Some(position) = self.elements.iter().position(|x| x == elem) {
                    action(elem, -1);
                    self.elements.swap_remove(position);
                }

                self.precedents.retain(|x| &x.0 != elem);
            }
        }
    }
}
