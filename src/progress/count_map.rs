use std::default::Default;
// use core::fmt::Debug;

// could be updated to be an Enum { Vec<(T, i64)>, HashMap<T, i64> } in the fullness of time
#[derive(Clone, Debug)]
pub struct CountMap<T> {
    updates: Vec<(T, i64)>
}

impl<T> Default for CountMap<T> {
    fn default() -> CountMap<T> { CountMap { updates: Vec::new() } }
}

impl<T:Eq+Clone+'static> CountMap<T> {
    #[inline(always)]
    pub fn update(&mut self, key: &T, val: i64) -> i64 {
        // if self.updates.len() > 100 { println!("perf: self.len() = {}", self.len()); }
        let mut remove_at = None;
        let mut found = false;
        let mut new_val = val;

        for (index, &mut (ref k, ref mut v)) in self.updates.iter_mut().enumerate() {
            if k.eq(key) {
                found = true;
                *v += val;
                new_val = *v;

                if new_val == 0 { remove_at = Some(index); }
            }
        }

        if !found && val != 0 { self.updates.push((key.clone(), val)); }
        if let Some(index) = remove_at { self.updates.swap_remove(index); }
        return new_val;
    }

    pub fn elements<'a>(&'a self) -> &'a Vec<(T, i64)> { &self.updates }
    pub fn clear(&mut self) { self.updates.clear(); }
    pub fn len(&self) -> usize { self.updates.len() }
    pub fn pop(&mut self) -> Option<(T, i64)> { self.updates.pop() }

    pub fn new() -> CountMap<T> { CountMap { updates: Vec::new() } }

    pub fn drain_into(&mut self, other: &mut CountMap<T>) {
        while let Some((ref key, val)) = self.updates.pop() {
            other.update(key, val);
        }
    }
}
