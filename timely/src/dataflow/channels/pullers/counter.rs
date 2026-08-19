//! A wrapper which accounts records pulled past in a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use crate::dataflow::channels::Message;
use crate::progress::{ChangeBatch, Stamp};
use crate::communication::Pull;
use crate::Accountable;

/// A wrapper which accounts records pulled past in a shared count map.
pub struct Counter<T, C, P> {
    pullable: P,
    consumed: Rc<RefCell<ChangeBatch<T>>>,
    phantom: ::std::marker::PhantomData<C>,
}

/// A guard type that updates the change batch counts on drop
pub struct ConsumedGuard<T: Ord + Clone + 'static> {
    consumed: Rc<RefCell<ChangeBatch<T>>>,
    stamp: Option<Stamp<T>>,
    record_count: i64,
}

impl<T:Ord+Clone+'static> ConsumedGuard<T> {
    #[inline]
    pub(crate) fn stamp(&self) -> &Stamp<T> {
        self.stamp.as_ref().unwrap()
    }
}

impl<T:Ord+Clone+'static> Drop for ConsumedGuard<T> {
    fn drop(&mut self) {
        // SAFETY: we're in a Drop impl, so this runs at most once
        let stamp = self.stamp.take().unwrap();
        let mut consumed = self.consumed.borrow_mut();
        for time in stamp.iter() {
            consumed.update(time.clone(), self.record_count);
        }
    }
}

impl<T:Ord+Clone+'static, C: Accountable, P: Pull<Message<T, C>>> Counter<T, C, P> {
    /// Retrieves the next timestamp and batch of data.
    #[inline]
    pub fn next(&mut self) -> Option<&mut Message<T, C>> {
        self.next_guarded().map(|(_guard, bundle)| bundle)
    }

    #[inline]
    pub(crate) fn next_guarded(&mut self) -> Option<(ConsumedGuard<T>, &mut Message<T, C>)> {
        if let Some(message) = self.pullable.pull() {
            let guard = ConsumedGuard {
                consumed: Rc::clone(&self.consumed),
                stamp: Some(message.stamp.clone()),
                record_count: message.data.record_count(),
            };
            Some((guard, message))
        }
        else { None }
    }
}

impl<T:Ord+Clone+'static, C, P: Pull<Message<T, C>>> Counter<T, C, P> {
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
