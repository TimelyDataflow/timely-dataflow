//! A wrapper which accounts records pulled past in a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use dataflow::channels::Content;
use progress::ChangeBatch;
use Pull;

/// A wrapper which accounts records pulled past in a shared count map.
pub struct Counter<T: Ord+Clone+'static, D, P: Pull<(T, Content<D>)>> {
    pullable: P,
    consumed: Rc<RefCell<ChangeBatch<T>>>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T:Ord+Clone+'static, D, P: Pull<(T, Content<D>)>> Counter<T, D, P> {
    /// Retrieves the next timestamp and batch of data.
    #[inline]
    pub fn next(&mut self) -> Option<(&T, &mut Content<D>)> {
        if let Some((ref time, ref mut data)) = *self.pullable.pull() {
            if data.len() > 0 {
                self.consumed.borrow_mut().update(time.clone(), data.len() as i64);
                Some((time, data))
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Ord+Clone+'static, D, P: Pull<(T, Content<D>)>> Counter<T, D, P> {
    /// Allocates a new `Counter` from a boxed puller.
    pub fn new(pullable: P) -> Self {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable: pullable,
            consumed: Rc::new(RefCell::new(ChangeBatch::new())),
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    pub fn consumed(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.consumed
    }
}
