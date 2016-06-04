//! A wrapper which counts the number of records pushed past and updates a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use progress::CountMap;
use dataflow::channels::Content;
use Push;

/// A wrapper which updates shared `counts` based on the number of records pushed.
pub struct Counter<T, D, P: Push<(T, Content<D>)>> {
    pushee: P,
    counts: Rc<RefCell<CountMap<T>>>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T, D, P: Push<(T, Content<D>)>> Push<(T, Content<D>)> for Counter<T, D, P> where T : Eq+Clone+'static {
    #[inline] 
    fn push(&mut self, message: &mut Option<(T, Content<D>)>) {
        if let Some((ref time, ref data)) = *message {
            self.counts.borrow_mut().update(time, data.len() as i64);
        }

        // only propagate `None` if dirty (indicates flush)
        if message.is_some() || self.counts.borrow().len() > 0 {
            self.pushee.push(message);            
        }
    }
}

impl<T, D, P: Push<(T, Content<D>)>> Counter<T, D, P> where T : Eq+Clone+'static {
    /// Allocates a new `Counter` from a pushee and shared counts.
    pub fn new(pushee: P, counts: Rc<RefCell<CountMap<T>>>) -> Counter<T, D, P> {
        Counter {
            pushee: pushee,
            counts: counts,
            phantom: ::std::marker::PhantomData,
        }
    }
    /// Extracts shared counts into `updates`.
    ///
    /// It is unclear why this method exists at the same time the counts are shared.
    /// Perhaps this should be investigated, and only one pattern used. Seriously.
    #[inline] pub fn pull_progress(&mut self, updates: &mut CountMap<T>) {
        while let Some((ref time, delta)) = self.counts.borrow_mut().pop() {
            updates.update(time, delta);
        }
    }
}
