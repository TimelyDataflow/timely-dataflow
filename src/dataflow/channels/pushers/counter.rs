//! A wrapper which counts the number of records pushed past and updates a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use progress::ChangeBatch;
use dataflow::channels::Bundle;
use Push;

/// A wrapper which updates shared `produced` based on the number of records pushed.
pub struct Counter<T: Ord, D, P: Push<Bundle<T, D>>> {
    pushee: P,
    produced: Rc<RefCell<ChangeBatch<T>>>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T, D, P> Push<Bundle<T, D>> for Counter<T, D, P> where T : Ord+Clone+'static, P: Push<Bundle<T, D>> {
    #[inline(always)]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        if let Some(message) = message {
            self.produced.borrow_mut().update(message.time.clone(), message.data.len() as i64);
        }

        // only propagate `None` if dirty (indicates flush)
        if message.is_some() || !self.produced.borrow_mut().is_empty() {
            self.pushee.push(message);
        }
    }
}

impl<T, D, P: Push<Bundle<T, D>>> Counter<T, D, P> where T : Ord+Clone+'static {
    /// Allocates a new `Counter` from a pushee and shared counts.
    pub fn new(pushee: P) -> Counter<T, D, P> {
        Counter {
            pushee,
            produced: Rc::new(RefCell::new(ChangeBatch::new())),
            phantom: ::std::marker::PhantomData,
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    #[inline(always)]
    pub fn produced(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.produced
    }
}
