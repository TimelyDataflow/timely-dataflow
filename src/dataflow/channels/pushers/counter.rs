//! A wrapper which counts the number of records pushed past and updates a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use progress::ChangeBatch;
use dataflow::channels::Content;
use Push;

/// A wrapper which updates shared `produced` based on the number of records pushed.
pub struct Counter<T: Ord, D, P: Push<(T, Content<D>)>> {
    pushee: P,
    produced: Rc<RefCell<ChangeBatch<T>>>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T: Ord, D, P: Push<(T, Content<D>)>> Push<(T, Content<D>)> for Counter<T, D, P> where T : Eq+Clone+'static {
    #[inline(always)]
    fn push(&mut self, message: &mut Option<(T, Content<D>)>) {
        if let Some((ref time, ref data)) = *message {
            self.produced.borrow_mut().update(time.clone(), data.len() as i64);
        }

        // only propagate `None` if dirty (indicates flush)
        if message.is_some() || !self.produced.borrow_mut().is_empty() {
            self.pushee.push(message);            
        }
    }
}

impl<T, D, P: Push<(T, Content<D>)>> Counter<T, D, P> where T : Ord+Clone+'static {
    /// Allocates a new `Counter` from a pushee and shared counts.
    pub fn new(pushee: P) -> Counter<T, D, P> {
        Counter {
            pushee: pushee,
            produced: Rc::new(RefCell::new(ChangeBatch::new())),
            phantom: ::std::marker::PhantomData,
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    #[inline(always)]
    pub fn produced(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.produced
    }
    // /// Extracts shared counts into `updates`.
    // ///
    // /// It is unclear why this method exists at the same time the counts are shared.
    // /// Perhaps this should be investigated, and only one pattern used. Seriously.
    // #[inline] pub fn pull_progress(&mut self, updates: &mut ChangeBatch<T>) {
    //     self.counts.borrow_mut().drain_into(updates);
    // }
}
