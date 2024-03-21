//! A wrapper which counts the number of records pushed past and updates a shared count map.

use std::marker::PhantomData;
use std::rc::Rc;
use std::cell::RefCell;

use crate::progress::{ChangeBatch, Timestamp};
use crate::dataflow::channels::Bundle;
use crate::communication::Push;
use crate::Container;

/// A wrapper which updates shared `produced` based on the number of records pushed.
#[derive(Debug)]
pub struct Counter<T, C, P: Push<Bundle<T, C>>> {
    pushee: P,
    produced: Rc<RefCell<ChangeBatch<T>>>,
    phantom: PhantomData<C>,
}

impl<T: Timestamp, C: Container, P> Push<Bundle<T, C>> for Counter<T, C, P> where P: Push<Bundle<T, C>> {
    #[inline]
    fn push(&mut self, message: &mut Option<Bundle<T, C>>) {
        if let Some(message) = message {
            self.produced.borrow_mut().update(message.time.clone(), message.data.len() as i64);
        }

        // only propagate `None` if dirty (indicates flush)
        if message.is_some() || !self.produced.borrow_mut().is_empty() {
            self.pushee.push(message);
        }
    }
}

impl<T, C, P: Push<Bundle<T, C>>> Counter<T, C, P> where T : Ord+Clone+'static {
    /// Allocates a new `Counter` from a pushee and shared counts.
    pub fn new(pushee: P) -> Counter<T, C, P> {
        Counter {
            pushee,
            produced: Rc::new(RefCell::new(ChangeBatch::new())),
            phantom: PhantomData,
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    #[inline]
    pub fn produced(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.produced
    }
}
