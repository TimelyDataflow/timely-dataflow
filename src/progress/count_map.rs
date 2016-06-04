//! A mapping from general types `T` to `i64`, with zero values absent.

use std::default::Default;
use std::slice::Iter;

/// Represents a map from `T` to `i64` with values mapping to zero removed.
///
/// The implementation is currently a `Vec<(T, i64)>` as the use cases are currently for small
/// buffers of updates rather than large storage. This could change as needs evolve.
#[derive(Clone, Debug)]
pub struct CountMap<T> {
    updates: Vec<(T, i64)>
}

impl<T> Default for CountMap<T> {
    fn default() -> CountMap<T> { CountMap { updates: Vec::new() } }
}

impl<T:Eq+Clone> CountMap<T> {
    /// Adds `val` to the value associated with `key`, returning the new value.
    #[inline]
    pub fn update(&mut self, key: &T, val: i64) -> i64 {
        if val != 0 {
        // if self.updates.len() > 100 { println!("perf: self.len() = {}", self.len()); }

            if let Some(index) = self.updates.iter().position(|&(ref k, _)| k.eq(key)) {
                self.updates[index].1 += val;
                let result = self.updates[index].1;
                if result == 0 {
                    self.updates.swap_remove(index);
                }
                result
            }
            else {
                self.updates.push((key.clone(), val));
                val
            }
        }
        else { 0 }
    }

    /// Extracts the `Vec<(T, i64)>` from the map, consuming it.
    pub fn into_inner(self) -> Vec<(T, i64)> { self.updates }
    /// Iterates over the contents of the map.
    pub fn iter(&self) -> Iter<(T, i64)> { self.updates.iter() }
    /// Clears the map.
    pub fn clear(&mut self) { self.updates.clear(); }
    /// The number of non-zero keys in the map.
    pub fn len(&self) -> usize { self.updates.len() }
    /// True iff all keys have value zero.
    pub fn is_empty(&self) -> bool { self.updates.is_empty() }
    /// Returns an element of the map, or `None` if it is empty.
    pub fn pop(&mut self) -> Option<(T, i64)> { self.updates.pop() }
    /// Allocates a new empty `CountMap`.
    pub fn new() -> CountMap<T> { CountMap { updates: Vec::new() } }
    /// Allocates a new `CountMap` with a single entry.
    pub fn new_from(key: &T, val: i64) -> CountMap<T> {
        let mut result = CountMap::new();
        result.update(key, val);
        result
    }

    /// Drains `self` into `other`.
    pub fn drain_into(&mut self, other: &mut CountMap<T>) {
        while let Some((ref key, val)) = self.updates.pop() {
            other.update(key, val);
        }
    }
    /// Performs a sequence of updates described by `iterator`.
    pub fn extend<I: Iterator<Item=(T, i64)>>(&mut self, iterator: I) {
        for (key, val) in iterator {
            self.update(&key, val);
        }
    }
}
