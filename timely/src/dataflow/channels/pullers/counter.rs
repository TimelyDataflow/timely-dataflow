//! A wrapper which accounts records pulled past in a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use crate::dataflow::channels::BundleCore;
use crate::progress::ChangeBatch;
use crate::communication::Pull;
use crate::Container;

/// A wrapper which accounts records pulled past in a shared count map.
pub struct Counter<T: Ord+Clone+'static, D, P: Pull<BundleCore<T, D>>> {
    pullable: P,
    consumed: Rc<RefCell<ChangeBatch<T>>>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T:Ord+Clone+'static, D: Container, P: Pull<BundleCore<T, D>>> Counter<T, D, P> {
    /// Retrieves the next timestamp and batch of data.
    #[inline]
    pub fn next(&mut self) -> Option<&mut BundleCore<T, D>> {
        if let Some(message) = self.pullable.pull() {
            if message.data.len() > 0 {
                self.consumed.borrow_mut().update(message.time.clone(), message.data.len() as i64);
                Some(message)
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Ord+Clone+'static, D, P: Pull<BundleCore<T, D>>> Counter<T, D, P> {
    /// Allocates a new `Counter` from a boxed puller.
    pub fn new(pullable: P) -> Self {
        Self::new_with_consumed(pullable, Rc::new(RefCell::new(ChangeBatch::new())))
    }

    /// Allocates a new [Counter] from a puller and a shared consumed handle.
    pub fn new_with_consumed(pullable: P, consumed: Rc<RefCell<ChangeBatch<T>>>) -> Self {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable,
            consumed,
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    pub fn consumed(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.consumed
    }
}
