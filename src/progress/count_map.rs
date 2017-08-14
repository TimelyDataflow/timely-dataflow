//! A mapping from general types `T` to `i64`, with zero values absent.

use std::default::Default;
use std::slice::Iter;

/// Represents a map from `T` to `i64` with values mapping to zero removed.
///
/// The implementation is currently a `Vec<(T, i64)>` as the use cases are currently for small
/// buffers of updates rather than large storage. This could change as needs evolve.
#[derive(Clone, Debug)]
pub struct CountMap<T> {
    peak: usize,
    updates: Vec<(T, i64)>,
    dirty: usize,
}

impl<T> Default for CountMap<T> {
    fn default() -> CountMap<T> { CountMap { peak: 0, updates: Vec::new(), dirty: 0 } }
}

impl<T:Ord+Clone> CountMap<T> {
    /// Adds `val` to the value associated with `key`, returning the new value.
    // #[inline(never)]
    pub fn update(&mut self, key: &T, val: i64) {
        self.updates.push((key.clone(), val));
        self.dirty += 1;
    }

    /// Drains the set of updates.
    #[inline(never)]
    pub fn drain(&mut self) -> ::std::vec::Drain<(T, i64)> {
        self.compact();
        self.updates.drain(..)
    }

    #[inline(never)]
    fn compact(&mut self) {
        if self.dirty > 0 && self.updates.len() > 0 {
            self.updates.sort_by(|x,y| x.0.cmp(&y.0));
            for i in 0 .. self.updates.len() - 1 {
                if self.updates[i].0 == self.updates[i+1].0 {
                    self.updates[i+1].1 += self.updates[i].1;
                    self.updates[i].1 = 0;
                }
            }
            self.updates.retain(|x| x.1 != 0);
        }
        self.dirty = 0;
    }

    /// Extracts the `Vec<(T, i64)>` from the map, consuming it.
    #[inline(never)]
    pub fn into_inner(mut self) -> Vec<(T, i64)> { 
        self.compact();
        self.updates 
    }
    /// Iterates over the contents of the map.
    #[inline(never)]
    pub fn iter(&mut self) -> Iter<(T, i64)> { 
        self.compact();
        self.updates.iter() 
    }
    /// Clears the map.
    #[inline(never)]
    pub fn clear(&mut self) { 
        self.updates.clear(); 
        self.dirty = 0; 
    }


    /// True iff all keys have value zero.
    #[inline(never)]
    pub fn is_empty(&mut self) -> bool {
        if self.dirty < self.updates.len() {
            false
        }
        else {
            self.compact();
            self.updates.is_empty()
        }
    }
    /// Returns an element of the map, or `None` if it is empty.
    // pub fn pop(&mut self) -> Option<(T, i64)> { self.updates.pop() }
    /// Allocates a new empty `CountMap`.
    pub fn new() -> CountMap<T> { 
        CountMap { 
            peak: 0, 
            updates: Vec::new(), 
            dirty: 0 
        } 
    }

    /// Allocates a new `CountMap` with a single entry.
    pub fn new_from(key: &T, val: i64) -> CountMap<T> {
        let mut result = CountMap::new();
        result.update(key, val);
        result
    }

    /// Drains `self` into `other`.
    #[inline(never)]
    pub fn drain_into(&mut self, other: &mut CountMap<T>) {
        while let Some((ref key, val)) = self.updates.pop() {
            other.update(key, val);
        }
    }
    /// Performs a sequence of updates described by `iterator`.
    #[inline(never)]
    pub fn extend<I: Iterator<Item=(T, i64)>>(&mut self, iterator: I) {
        for (key, val) in iterator {
            self.update(&key, val);
        }
    }
}
