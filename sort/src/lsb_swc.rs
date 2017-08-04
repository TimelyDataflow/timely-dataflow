// use std::slice;

use ::{Unsigned, RadixSorter, RadixSorterBase};
use stash::Stash;
use swc_buffer::SWCBuffer;
use batched_vec::BatchedVecX256;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 4096 / 64 }}
}

/// A few buffers capable of radix sorting by least significant byte.
///
/// The sorter allows the use of multiple different key bytes, determined by a type `U: Unsigned`.
/// Currently, one is allowed to mix and match these as records are pushed, which may be a design
/// bug.
pub struct Sorter<T> {
    shuffler: Shuffler<T>,
}

impl<T, U: Unsigned> RadixSorter<T, U> for Sorter<T> {

    #[inline(always)]
    fn push<F: Fn(&T)->U>(&mut self, element: T, key: &F) {
        self.shuffler.push(element, &|x| (key(x).as_u64() % 256) as u8);
    }

    #[inline]
    fn push_batch<F: Fn(&T)->U>(&mut self, batch: Vec<T>, key: &F) {
        self.shuffler.push_batch(batch,  &|x| (key(x).as_u64() % 256) as u8);
    }

    fn finish_into<F: Fn(&T)->U>(&mut self, target: &mut Vec<Vec<T>>, key: &F) {
        self.shuffler.finish_into(target);
        for byte in 1..(<U as Unsigned>::bytes()) { 
            self.reshuffle(target, &|x| ((key(x).as_u64() >> (8 * byte)) % 256) as u8);
        }
    }
}

impl<T> RadixSorterBase<T> for Sorter<T> {
    fn new() -> Self { Sorter { shuffler: Shuffler::new() } }
    fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize) {
        self.shuffler.stash.rebalance(buffers, intended);
    }
}

impl<T> Sorter<T> {
    #[inline(always)]
    fn reshuffle<F: Fn(&T)->u8>(&mut self, buffers: &mut Vec<Vec<T>>, key: &F) {
        for buffer in buffers.drain(..) {
            self.shuffler.push_batch(buffer, key);
        }
        self.shuffler.finish_into(buffers);
    }
}

// The following is a software write-combining implementation. This technique is meant to cut
// down on the the amount of memory traffic due to non-full cache lines moving around. at the
// moment, on my laptop, it doesn't seem to improve things. But! I hope that in the fullness of
// time this changes, either because the implementation improves, or I get a laptop with seventy
// bazillion cores.

pub struct Shuffler<T> {
    buffer: SWCBuffer<T>,
    // staged: Vec<T>,     // ideally: 256 * number of T element per cache line.
    // counts: [u8; 256],

    buckets: BatchedVecX256<T>,
    stash: Stash<T>,      // spare segments
}

impl<T> Shuffler<T> {
    fn new() -> Shuffler<T> {
        // let staged = Vec::with_capacity(256 * per_cache_line!(T));

        // // looks like this is often cache-line aligned; yay!
        // let addr: usize = unsafe { ::std::mem::transmute(staged.as_ptr()) };
        // assert_eq!(addr % 64, 0);

        Shuffler {
            buffer: SWCBuffer::new(),
            // staged: staged,
            // counts: [0; 256],
            buckets: BatchedVecX256::new(),
            stash: Stash::new(lines_per_page!() * per_cache_line!(T)),
        }
    }

    fn push_batch<F: Fn(&T)->u8>(&mut self, mut elements: Vec<T>, key: &F) {
        for element in elements.drain(..) {
            self.push(element, key);
        }

        self.stash.give(elements);
    }

    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, key: &F) {

        let byte = key(&element) as usize;

        if self.buffer.full(byte) {
            self.buffer.drain_into(byte, &mut self.buckets.get_mut(byte), &mut self.stash);
        }

        self.buffer.push(element, byte);
    }

    fn finish_into(&mut self, target: &mut Vec<Vec<T>>) {

        // This does the slightly clever thing of moving allocated data to `target` and then checking
        // if there is room for remaining elements in `self.staged`, which do not yet have an allocation.
        // If there is room, we just copy them into the buffer, potentially saving on allocation churn.

        for byte in 0..256 {
            self.buckets.get_mut(byte).finish_into(target);

            if target.last().map(|x| x.capacity() - x.len() >= self.buffer.count(byte)) != Some(true) {
                target.push(self.stash.get());
            }

            let last = target.last_mut().unwrap();
            self.buffer.drain_into_vec(byte, last, &mut self.stash);
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

        use {RadixSorter, RadixSorterBase};
        let mut sorter = super::Sorter::new();

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

        use {RadixSorter, RadixSorterBase};
        let mut sorter = super::Sorter::new();

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