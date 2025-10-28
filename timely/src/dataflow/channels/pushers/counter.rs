//! A wrapper which counts the number of records pushed past and updates a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use crate::progress::ChangeBatch;
use crate::dataflow::channels::Message;
use crate::communication::Push;
use crate::Accountable;

/// A wrapper which updates shared `produced` based on the number of records pushed.
#[derive(Debug)]
pub struct Counter<T, P> {
    pushee: P,
    produced: Rc<RefCell<ChangeBatch<T>>>,
}

impl<T: Clone+Ord, C: Accountable, P> Push<Message<T, C>> for Counter<T, P> where P: Push<Message<T, C>> {
    #[inline]
    fn push(&mut self, message: &mut Option<Message<T, C>>) {
        if let Some(message) = message {
            self.produced.borrow_mut().update(message.time.clone(), message.data.record_count());
        }

        // only propagate `None` if dirty (indicates flush)
        if message.is_some() || !self.produced.borrow_mut().is_empty() {
            self.pushee.push(message);
        }
    }
}

impl<T, P> Counter<T, P> where T : Ord+Clone+'static {
    /// Allocates a new `Counter` from a pushee and shared counts.
    pub fn new(pushee: P) -> Counter<T, P> {
        Counter {
            pushee,
            produced: Rc::new(RefCell::new(ChangeBatch::new())),
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    #[inline]
    pub fn produced(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.produced
    }
}
