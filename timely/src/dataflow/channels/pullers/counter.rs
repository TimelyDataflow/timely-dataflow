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

/// A guard type that updates the change batch counts on drop
pub struct ConsumedGuard<'a, T: Ord + Clone + 'static> {
    consumed: &'a Rc<RefCell<ChangeBatch<T>>>,
    time: Option<T>,
    len: usize,
}

impl<'a, T:Ord+Clone+'static> Drop for ConsumedGuard<'a, T> {
    fn drop(&mut self) {
        self.consumed.borrow_mut().update(self.time.take().unwrap(), self.len as i64);
    }
}

impl<T:Ord+Clone+'static, D: Container, P: Pull<BundleCore<T, D>>> Counter<T, D, P> {
    /// Retrieves the next timestamp and batch of data.
    #[inline]
    pub fn next(&mut self) -> Option<&mut BundleCore<T, D>> {
        self.next_guarded().map(|(_guard, bundle)| bundle)
    }

    #[inline]
    pub(crate) fn next_guarded(&mut self) -> Option<(ConsumedGuard<'_, T>, &mut BundleCore<T, D>)> {
        if let Some(message) = self.pullable.pull() {
            if message.data.len() > 0 {
                let guard = ConsumedGuard {
                    consumed: &self.consumed,
                    time: Some(message.time.clone()),
                    len: message.data.len(),
                };
                Some((guard, message))
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Ord+Clone+'static, D, P: Pull<BundleCore<T, D>>> Counter<T, D, P> {
    /// Allocates a new `Counter` from a boxed puller.
    pub fn new(pullable: P) -> Self {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable,
            consumed: Rc::new(RefCell::new(ChangeBatch::new())),
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    pub fn consumed(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.consumed
    }
}
