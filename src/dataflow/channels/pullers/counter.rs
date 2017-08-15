//! A wrapper which accounts records pulled past in a shared count map.

use dataflow::channels::Content;
use progress::ChangeBatch;
use Pull;

/// A wrapper which accounts records pulled past in a shared count map.
pub struct Counter<T: Ord+Clone+'static, D> {
    pullable: Box<Pull<(T, Content<D>)>>,
    consumed: ChangeBatch<T>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T:Ord+Clone+'static, D> Counter<T, D> {
    /// Retrieves the next timestamp and batch of data.
    #[inline]
    pub fn next(&mut self) -> Option<(&T, &mut Content<D>)> {
        if let Some((ref time, ref mut data)) = *self.pullable.pull() {
            if data.len() > 0 {
                self.consumed.update(time.clone(), data.len() as i64);
                Some((time, data))
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Ord+Clone+'static, D> Counter<T, D> {
    /// Allocates a new `Counter` from a boxed puller.
    pub fn new(pullable: Box<Pull<(T, Content<D>)>>) -> Counter<T, D> {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable: pullable,
            consumed: ChangeBatch::new(),
        }
    }
    /// Extracts progress information into `consumed`. 
    pub fn pull_progress(&mut self, consumed: &mut ChangeBatch<T>) {
        for (time, value) in self.consumed.drain() {
            consumed.update(time, value);
        }
    }
}
