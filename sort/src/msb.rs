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

use ::Unsigned;
use stash::Stash;
use batched_vec::BatchedVecX256;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::min(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 1024 }}
}

// At any point in time, we have some outstanding work to do, 
pub struct RadixSorter<T> {

    work: Vec<(usize, Vec<Vec<T>>)>,    // a stack of (level, items) of work to do.

    done_list: Vec<Vec<T>>,             // where we put the assembled data.
    done_tail: Vec<T>,

    buckets: BatchedVecX256<T>,
    stage: Vec<T>,                      // where we may have to stage data if it is too large.
    stash: Stash<T>,                 // empty buffers we might be able to use.
}

impl<T> RadixSorter<T> {

    pub fn new() -> Self {
        RadixSorter {
            work: Vec::new(),
            done_list: Vec::new(),
            done_tail: Vec::new(),
            buckets: BatchedVecX256::new(),
            stage: Vec::new(),
            stash: Stash::new(1 << 10),
        }
    }

    #[inline(always)]
    pub fn push<U: Unsigned, F: Fn(&T)->U>(&mut self, element: T, bytes: F) {
        let depth = U::bytes();
        let byte = (bytes(&element).as_u64() >> (8 * depth)) as u8;
        self.buckets.get_mut(byte).push(element, &mut self.stash);
    }

    pub fn push_batch<U: Unsigned, F: Fn(&T)->U>(&mut self, mut batch: Vec<T>, bytes: F) {
        for element in batch.drain(..) {
            self.push(element, |x| bytes(x));
        }
        self.stash.give(batch);
    }

    pub fn finish<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut [T])>(&mut self, bytes: F, action: L) -> Vec<Vec<T>> {

        let depth = U::bytes() - 1;
        for byte in (0 .. 256).rev() {
            let mut bucket = self.buckets.get_mut(byte as u8); 
            if !bucket.is_empty() {
                self.work.push((depth, bucket.finish(&mut self.stash)));
            }
        }

        while let Some((depth, mut list)) = self.work.pop() {
            self.ingest(&mut list, &bytes, depth, &action);
        }

        if self.done_tail.len() > 0 { 
            let empty = self.empty();
            self.done_list.push(replace(&mut self.done_tail, empty)); 
        }

        ::std::mem::replace(&mut self.done_list, Vec::new())
    }

    pub fn recycle(&mut self, mut buffers: Vec<Vec<T>>) {
        for mut buffer in buffers.drain(..) {
            buffer.clear();
            self.stash.give(buffer);
        }
    }

    fn empty(&mut self) -> Vec<T> {
        self.stash.get()
    }

    /// Radix sorts a sequence of buffers, possibly stopping early on small batches and calling `action`.
    ///
    /// The intent of `sort` is to allow us to do top-down radix sorting with the option to exit early if
    /// the sizes of elements to sort are small enough. Of course, just exiting early doesn't sort the data,
    /// so we have an `action` you get to apply to finish things off. We could make this sort by radix, but
    /// there are several use cases where we need to follow up with additional sorting / compaction and would
    /// like to hook this clean-up method anyhow.
    pub fn sort<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut [T])>(&mut self, source: &mut Vec<Vec<T>>, bytes: F, action: L) {
        self.work.push((U::bytes(), replace(source, Vec::new())));
        while let Some((depth, mut list)) = self.work.pop() {
            self.ingest(&mut list, &bytes, depth, &action);
        }

        if self.done_tail.len() > 0 { 
            let empty = self.empty();
            self.done_list.push(replace(&mut self.done_tail, empty)); 
        }
        ::std::mem::swap(&mut self.done_list, source);
    }

    // digests a list of batches, which we assume (for some reason) to be densely packed.
    fn ingest<U: Unsigned, F: Fn(&T)->U, L: Fn(&mut [T])>(&mut self, source: &mut Vec<Vec<T>>, bytes: &F, depth: usize, action: &L) {

        if source.len() > 1 {

            // in this case, we should have a non-trivial number of elements, and so we can spend some effort
            // looking at the current radix and going to work. of course, we can only do this if bytes remain;
            // otherwise we will need to stage the data in self.stage and apply `action` to it.

            if depth > 0 {

                let depth = depth - 1;

                // push all of source into lists and tails.
                for mut batch in source.drain(..) {
                    for element in batch.drain(..) {
                        let byte = (bytes(&element).as_u64() >> (8 * depth)) as u8;
                        self.buckets.get_mut(byte).push(element, &mut self.stash);
                    }
                    self.stash.give(batch);
                }

                // we are actually doing this is reverse, so that popping the stack goes in the right order.
                for byte in (0 .. 256).rev() {
                    let mut bucket = self.buckets.get_mut(byte as u8);
                    if !bucket.is_empty() {
                        self.work.push((depth, bucket.finish(&mut self.stash)));
                    }
                }
            }
            else {
                for mut batch in source.drain(..) {
                    for element in batch.drain(..) {
                        self.stage.push(element);
                    }

                    self.stash.give(batch);
                }

                action(&mut self.stage[..]);

                for element in self.stage.drain(..) {
                    if self.done_tail.len() == self.done_tail.capacity() {
                        // let empty = self.empty();
                        let empty = self.stash.get();
                        let tail = replace(&mut self.done_tail, empty);
                        if tail.len() > 0 {
                            self.done_list.push(tail);
                        }
                    }
                    self.done_tail.push(element);
                }
            }

        }
        else if let Some(mut batch) = source.pop() {

            action(&mut batch[..]);

            // ideally we would move part of this batch into done_tail, and the rest within itself, 
            // using at most two memcpys. I don't know how to do this without `unsafe`. Could do that.

            let available = self.done_tail.capacity() - self.done_tail.len();
            let to_move = ::std::cmp::min(available, batch.len());

            self.done_tail.extend(batch.drain(.. to_move));

            if batch.len() > 0 {
                let tail = replace(&mut self.done_tail, batch);
                if tail.len() > 0 {
                    self.done_list.push(tail);
                }
            }
            else {
                self.stash.give(batch);
            }
        }
    }
}

#[test]
fn test_msb() {

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

    let mut sorter = RadixSorter::new();

    sorter.sort(&mut vector, |&x| x, |xs| xs.sort());

    let mut prev = 0;
    for item in vector.drain(..).flat_map(|batch| batch.into_iter()) {
        assert!(prev < item);
        prev = item;
    }

    assert!(prev == size);
}
