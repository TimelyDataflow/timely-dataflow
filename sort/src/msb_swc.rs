//! Most-significant bit (MSB) radix sorting.
//!
//! MSB radix sorting works by partitioning elements first by their highest byte, and then recursively
//! processing each part. Our plan is to do this in a depth-first manner, to minimize the outlay of 
//! partially empty buffers and such.
//! 
//! One advantage of MSB radix sorting is that we can stop at any point and fall into a traditional sort
//! implementation. I expect this to be useful, in that many instances of radix sorting are based on a 
//! hash of keys, and still require a final sort in any case.
//!
//! This advantage is likely also an important performance detail: we should eventually fall into a more 
//! traditional sort if the number of elements becomes small, as our implementation will have non-trivial
//! (i.e. 256) overhead for each invocation. Once we have less than some fixed amount of work (e.g. one 
//! buffer's full of elements) we should fall into the final sort.

use ::std::mem::replace;
// use ::std::collections::VecDeque;

use ::{Unsigned, RadixSorter, RadixSorterBase};
use stash::Stash;
use batched_vec::{BatchedVec, BatchedVecX256};
use swc_buffer::SWCBuffer;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 4096 / 64 }}
}

/// A "most-significant byte" (MSB) radix sorter. 
///
/// This type manages buffers and provides logic for sorting batches of records using binary keys, in a 
/// "top down" or "most-significant byte" manner. One advantage of this approach is that it can exit early
/// if batch sizes become small, avoiding radix scans over the data. 
///
/// The type also allows the user to hook this the early exit functionality, as the finalizing action before 
/// the early exit can be a great time to consolidate or deduplicate records if this is desired, and can be
/// much more efficient than trying to do the same after the fact (often the radix key is not the true key, 
/// and one must re-sort by the actual key). See the `finish_into_and` and `sort_and` methods.
pub struct Sorter<T> {

    buffer: SWCBuffer<T>,

    work: Vec<Work<T>>,    // a stack of (level, items) of work to do.

    done: BatchedVec<T>,                // where we put the assembled data.
    buckets: BatchedVecX256<T>,

    stage: Vec<T>,                      // where we may have to stage data if it is too large.
    stash: Stash<T>,                    // empty buffers we might be able to use.
}

/// Describes work queued (stacked, actually) up to perform.
pub enum Work<T> {
    /// Indicates sorted and compacted results, which simply need to be moved into place.
    Done(Vec<T>),
    /// Indicates sorting still to be done, by byte position and list of elements to sort.
    Sort(usize, Vec<Vec<T>>),
}

impl<T, U: Unsigned> RadixSorter<T, U> for Sorter<T> {

    #[inline(always)]
    fn push<F: Fn(&T)->U>(&mut self, element: T, bytes: &F) {
        let depth = U::bytes() - 1;
        let byte = ((bytes(&element).as_u64() >> (8 * depth)) & 0xFF) as usize;

        // write the element to our scratch buffer space and consider it taken care of.
        if self.buffer.full(byte) {
            self.buffer.drain_into(byte, &mut self.buckets.get_mut(byte), &mut self.stash);
        }

        self.buffer.push(element, byte);
    }

    #[inline]
    fn push_batch<F: Fn(&T)->U>(&mut self, mut batch: Vec<T>, bytes: &F) {
        for element in batch.drain(..) {
            self.push(element, &|x| bytes(x));
        }
        self.stash.give(batch);
    }

    fn finish_into<F: Fn(&T)->U>(&mut self, target: &mut Vec<Vec<T>>, bytes: &F) {
        self.finish_into_and(target, bytes, |vec| if vec.len() > 1 { vec.sort_by(|x,y| bytes(x).cmp(&bytes(y))) });
    }

    fn sort<F: Fn(&T)->U>(&mut self, batches: &mut Vec<Vec<T>>, bytes: &F) {
        self.sort_and(batches, bytes, |vec| vec.sort_by(|x,y| bytes(x).cmp(&bytes(y))));
    }
}

impl<T> RadixSorterBase<T> for Sorter<T> {
    fn new() -> Self {
        Sorter {
            buffer: SWCBuffer::new(),
            work: Vec::new(),
            done: BatchedVec::new(),
            buckets: BatchedVecX256::new(),
            stage: Vec::new(),
            stash: Stash::new(lines_per_page!() * per_cache_line!(T)),
        }
    }
    fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize) {
        self.stash.rebalance(buffers, intended);
    }
}

impl<T> Sorter<T> {

    /// Finishes the sorting for the session, using the supplied finalizing action when given the option to exit early.
    #[inline(always)]
    pub fn finish_into_and<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut Vec<T>)>(&mut self, target: &mut Vec<Vec<T>>, bytes: F, action: L) {

        let depth = U::bytes() - 1;

        // temp space to `finish_into`.
        let mut temp = Vec::new();

        for byte in (0 .. 256).rev() {
            let mut bucket = self.buckets.get_mut(byte); 
            self.buffer.drain_into(byte, &mut bucket, &mut self.stash);
            if !bucket.is_empty() {
                bucket.finish_into(&mut temp);
                if temp.len() == 1 {
                    let mut buffer = temp.pop().unwrap();
                    action(&mut buffer);
                    self.work.push(Work::Done(buffer));
                }
                else {
                    self.work.push(Work::Sort(depth, temp));
                    temp = Vec::new();
                }
            }
        }

        self.grind(&bytes, &action);
        self.done.ref_mut().finish_into(target)
    }

    /// Radix sorts a sequence of buffers, possibly stopping early on small batches and calling `action`.
    ///
    /// The intent of `sort_and` is to allow us to do top-down radix sorting with the option to exit early if
    /// the sizes of elements to sort are small enough. Of course, just exiting early doesn't sort the data,
    /// so we have an `action` you get to apply to finish things off. We could make this sort by radix, but
    /// there are several use cases where we need to follow up with additional sorting / compaction and would
    /// like to hook this clean-up method anyhow.
    #[inline(always)]
    pub fn sort_and<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut Vec<T>)>(&mut self, source: &mut Vec<Vec<T>>, bytes: F, action: L) {
        if source.len() > 1 {
            self.work.push(Work::Sort(U::bytes(), replace(source, Vec::new())));
            self.grind(&bytes, &action);
            self.done.ref_mut().finish_into(source);
        }
        else if source.len() == 1 {
            action(&mut source[0]);
        }
    }

    #[inline(always)]
    fn grind<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut Vec<T>)>(&mut self, bytes: &F, action: &L) {

        while let Some(work) = self.work.pop() {
            match work {
                // TODO : Should this just be a push, not a copy? Risk if sparsely occupied.
                Work::Done(batch) => self.done.ref_mut().push_vec(batch, &mut self.stash),
                Work::Sort(depth, mut list) => {
                    self.ingest(&mut list, bytes, depth, action);
                }
            }
        }
    }

    // digests a list of batches, which we assume (for some reason) to be densely packed.
    #[inline(always)]
    fn ingest<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut Vec<T>)>(&mut self, source: &mut Vec<Vec<T>>, bytes: &F, depth: usize, action: &L) {

        // in this case, we should have a non-trivial number of elements, and so we can spend some effort
        // looking at the current radix and going to work. of course, we can only do this if bytes remain;
        // otherwise we will need to stage the data in self.stage and apply `action` to it.

        if depth > 0 {

            let depth = depth - 1;

            // push all of source into lists and tails.
            for mut batch in source.drain(..) {
                for element in batch.drain(..) {
                    let byte = ((bytes(&element).as_u64() >> (8 * depth)) & 0xFF) as usize;
                    if self.buffer.full(byte) {
                        self.buffer.drain_into(byte, &mut self.buckets.get_mut(byte), &mut self.stash);
                    }
                    self.buffer.push(element, byte);
                }
                self.stash.give(batch);
            }

            let mut temp = Vec::new();
            for byte in (0 .. 256).rev() {
                let mut bucket = self.buckets.get_mut(byte);
                self.buffer.drain_into(byte, &mut bucket, &mut self.stash);
                if !bucket.is_empty() {
                    bucket.finish_into(&mut temp);
                    if temp.len() == 1 {
                        let mut buffer = temp.pop().unwrap();
                        action(&mut buffer);
                        self.work.push(Work::Done(buffer));
                    }
                    else {
                        self.work.push(Work::Sort(depth, temp));
                        temp = Vec::new();
                    }
                }
            }
        }
        else {
            let len = source.iter().map(|x| x.len()).sum();
            self.stage.reserve(len);
            for mut batch in source.drain(..) {
                for element in batch.drain(..) {
                    self.stage.push(element);
                }
                self.stash.give(batch);
            }

            action(&mut self.stage);

            for element in self.stage.drain(..) {
                self.done.ref_mut().push(element, &mut self.stash);
            }
        }
    }
}

mod test {
    #[test]
    fn test_msb() {

        use ::std::mem::replace;

        let size = (1 << 20) as usize;
        let mut batch = Vec::with_capacity(1 << 10);
        let mut vector = Vec::new();
        for i in 1..(size+1) {
            if batch.len() == batch.capacity() {
                vector.push(replace(&mut batch, Vec::with_capacity(1 << 10)));
            }
            batch.push(i);
        }
        vector.push(replace(&mut batch, Vec::with_capacity(1 << 10)));

        use {RadixSorter, RadixSorterBase};
        let mut sorter = super::Sorter::new();

        sorter.sort(&mut vector, &|&x| x);

        let mut prev = 0;
        for item in vector.drain(..).flat_map(|batch| batch.into_iter()) {
            assert!(prev < item);
            prev = item;
        }

        assert!(prev == size);
    }
}
