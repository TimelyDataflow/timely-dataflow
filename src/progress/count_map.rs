//! A mapping from general types `T` to `i64`, with zero values absent.

use std::default::Default;
use std::slice::Iter;

// could be updated to be an Enum { Vec<(T, i64)>, HashMap<T, i64> } in the fullness of time
#[derive(Clone, Debug)]
pub struct CountMap<T> {
    updates: Vec<(T, i64)>
}

impl<T> Default for CountMap<T> {
    fn default() -> CountMap<T> { CountMap { updates: Vec::new() } }
}

impl<T:Eq+Clone> CountMap<T> {
    #[inline(always)]
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

    pub fn into_inner(self) -> Vec<(T, i64)> { self.updates }
    pub fn iter(&self) -> Iter<(T, i64)> { self.updates.iter() }
    pub fn clear(&mut self) { self.updates.clear(); }
    pub fn len(&self) -> usize { self.updates.len() }
    pub fn is_empty(&self) -> bool { self.updates.is_empty() }
    pub fn pop(&mut self) -> Option<(T, i64)> { self.updates.pop() }

    pub fn new() -> CountMap<T> { CountMap { updates: Vec::new() } }
    pub fn new_from(key: &T, val: i64) -> CountMap<T> {
        let mut result = CountMap::new();
        result.update(key, val);
        result
    }

    pub fn drain_into(&mut self, other: &mut CountMap<T>) {
        while let Some((ref key, val)) = self.updates.pop() {
            other.update(key, val);
        }
    }
    pub fn extend<I: Iterator<Item=(T, i64)>>(&mut self, iterator: I) {
        for (key, val) in iterator {
            self.update(&key, val);
        }
    }
}
