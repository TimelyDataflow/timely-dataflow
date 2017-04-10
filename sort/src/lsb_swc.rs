use std::slice;

use ::Unsigned;
use stash::Stash;
use batched_vec::BatchedVecX256;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 2 * 4096 / 64 }}
}

/// A few buffers capable of radix sorting by least significant byte.
///
/// The sorter allows the use of multiple different key bytes, determined by a type `U: Unsigned`.
/// Currently, one is allowed to mix and match these as records are pushed, which may be a design
/// bug.
pub struct RadixSorter<T> {
    shuffler: RadixShuffler<T>,
}

impl<T> RadixSorter<T> {
    /// Constructs a new radix sorter.
    pub fn new() -> RadixSorter<T> {
        RadixSorter {
            shuffler: RadixShuffler::new(),
        }
    }
    /// Pushes a sequence of elements into the sorter.
    #[inline]
    pub fn extend<U: Unsigned, F: Fn(&T)->U, I: Iterator<Item=T>>(&mut self, iterator: I, function: &F) {
        for element in iterator {
            self.push(element, function);
        }
    }
    /// Pushes a single element into the sorter.
    #[inline]
    pub fn push<U: Unsigned, F: Fn(&T)->U>(&mut self, element: T, function: &F) {
        self.shuffler.push(element, &|x| (function(x).as_u64() % 256) as u8);
    }
    /// Pushes a batch of elements into the sorter, and transfers ownership of the containing allocation.
    #[inline]
    pub fn push_batch<U: Unsigned, F: Fn(&T)->U>(&mut self, batch: Vec<T>, function: &F) {
        self.shuffler.push_batch(batch,  &|x| (function(x).as_u64() % 256) as u8);
    }
    /// Sorts a sequence of batches, re-using the allocations where possible and re-populating `batches`.
    pub fn sort<U: Unsigned, F: Fn(&T)->U>(&mut self, batches: &mut Vec<Vec<T>>, function: &F) {
        for batch in batches.drain(..) { 
            self.push_batch(batch, function); 
        }
        self.finish_into(batches, function);
    }
    /// Finishes a sorting session by allocating and populating a sequence of batches.
    pub fn finish<U: Unsigned, F: Fn(&T)->U>(&mut self, function: &F) -> Vec<Vec<T>> {
        let mut result = Vec::new();
        self.finish_into(&mut result, function);
        result
    }
    /// Finishes a sorting session by populating a supplied sequence.
    pub fn finish_into<U: Unsigned, F: Fn(&T)->U>(&mut self, target: &mut Vec<Vec<T>>, function: &F) {
        self.shuffler.finish_into(target);
        for byte in 1..(<U as Unsigned>::bytes()) { 
            self.reshuffle(target, &|x| ((function(x).as_u64() >> (8 * byte)) % 256) as u8);
        }
    }
    /// Consumes supplied buffers for future re-use by the sorter.
    ///
    /// This method is equivalent to `self.rebalance(buffers, usize::max_value())`.
    pub fn recycle(&mut self, buffers: &mut Vec<Vec<T>>) {
        self.rebalance(buffers, usize::max_value());
    }
    /// Either consumes from or pushes into `buffers` to leave `intended` spare buffers with the sorter.
    pub fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize) {
        self.shuffler.stash.rebalance(buffers, intended);
    }
    #[inline(always)]
    fn reshuffle<F: Fn(&T)->u8>(&mut self, buffers: &mut Vec<Vec<T>>, function: &F) {
        for buffer in buffers.drain(..) {
            self.shuffler.push_batch(buffer, function);
        }
        self.shuffler.finish_into(buffers);
    }
}

// The following is a software write-combining implementation. This technique is meant to cut
// down on the the amount of memory traffic due to non-full cache lines moving around. at the
// moment, on my laptop, it doesn't seem to improve things. But! I hope that in the fullness of
// time this changes, either because the implementation improves, or I get a laptop with seventy
// bazillion cores.

pub struct RadixShuffler<T> {
    staged: Vec<T>,     // ideally: 256 * number of T element per cache line.
    counts: [u8; 256],

    buckets: BatchedVecX256<T>,
    stash: Stash<T>,      // spare segments
}

impl<T> RadixShuffler<T> {
    fn new() -> RadixShuffler<T> {
        let staged = Vec::with_capacity(256 * per_cache_line!(T));

        // looks like this is often cache-line aligned; yay!
        let addr: usize = unsafe { ::std::mem::transmute(staged.as_ptr()) };
        assert_eq!(addr % 64, 0);

        RadixShuffler {
            staged: staged,
            counts: [0; 256],
            buckets: BatchedVecX256::new(),
            stash: Stash::new(lines_per_page!() * per_cache_line!(T)),
        }
    }

    fn push_batch<F: Fn(&T)->u8>(&mut self, mut elements: Vec<T>, function: &F) {
        for element in elements.drain(..) {
            self.push(element, function);
        }

        self.stash.give(elements);
    }

    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, function: &F) {

        let byte = function(&element);

        // write the element to our scratch buffer space and consider it taken care of.
        unsafe {

            // if we have saturated the buffer for byte, flush it out
            if *self.counts.get_unchecked(byte as usize) as usize == per_cache_line!(T) {
                let staged_elements = slice::from_raw_parts(
                    self.staged.as_ptr().offset(per_cache_line!(T) as isize * byte as isize),
                    per_cache_line!(T)
                );

                self.buckets.get_mut(byte).push_all(staged_elements, &mut self.stash);

                self.counts[byte as usize] = 0;
            }

            // self.staged[byte * stride + self.counts[byte]] = element; self.counts[byte] += 1
            let offset = per_cache_line!(T) as isize * byte as isize + *self.counts.get_unchecked(byte as usize) as isize;
            ::std::ptr::write(self.staged.as_mut_ptr().offset(offset), element);
            *self.counts.get_unchecked_mut(byte as usize) += 1;
        }
    }

    fn finish_into(&mut self, target: &mut Vec<Vec<T>>) {

        // This does the slightly clever thing of moving allocated data to `target` and then checking
        // if there is room for remaining elements in `self.staged`, which do not yet have an allocation.
        // If there is room, we just copy them into the buffer, potentially saving on allocation churn.

        for byte in 0..256 {
            self.buckets.get_mut(byte as u8).finish_into(target);

            if target.last().map(|x| x.capacity() - x.len() >= self.counts[byte] as usize) != Some(true) {
                target.push(self.stash.get());
            }

            let last = target.last_mut().unwrap();
            for i in 0..self.counts[byte] {
                unsafe {
                    last.push(::std::ptr::read(self.staged.as_mut_ptr().offset(per_cache_line!(T) as isize * byte as isize + i as isize)));
                }
            }
            self.counts[byte] = 0;
        }
    }
}

mod test {

    #[test]
    fn test1() {

        let size = 10;

        let mut vector = Vec::<usize>::with_capacity(size);
        for index in 0..size {
            vector.push(index);
        }
        for index in 0..size {
            vector.push(size - index);
        }

        let mut sorter = super::RadixSorter::new();

        for &element in &vector {
            sorter.push(element, &|&x| x);
        }

        vector.sort();

        let mut result = Vec::new();
        for element in sorter.finish(&|&x| x).into_iter().flat_map(|x| x.into_iter()) {
            result.push(element);
        }

        assert_eq!(result, vector);
    }


    #[test]
    fn test_large() {

        let size = 1_000_000;

        let mut vector = Vec::<[usize; 16]>::with_capacity(size);
        for index in 0..size {
            vector.push([index; 16]);
        }
        for index in 0..size {
            vector.push([size - index; 16]);
        }

        let mut sorter = super::RadixSorter::new();

        for &element in &vector {
            sorter.push(element, &|&x| x[0]);
        }

        vector.sort_by(|x, y| x[0].cmp(&y[0]));

        let mut result = Vec::new();
        for element in sorter.finish(&|&x| x[0]).into_iter().flat_map(|x| x.into_iter()) {
            result.push(element);
        }

        assert_eq!(result, vector);
    }

}
