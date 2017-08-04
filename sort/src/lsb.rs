use ::{Unsigned, RadixSorter, RadixSorterBase};
use stash::Stash;
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
    fn push<F: Fn(&T)->U>(&mut self, element: T, function: &F) {
        self.shuffler.push(element, &|x| (function(x).as_u64() % 256) as u8);
    }

    #[inline]
    fn push_batch<F: Fn(&T)->U>(&mut self, batch: Vec<T>, function: &F) {
        self.shuffler.push_batch(batch,  &|x| (function(x).as_u64() % 256) as u8);
    }

    fn finish_into<F: Fn(&T)->U>(&mut self, target: &mut Vec<Vec<T>>, function: &F) {
        self.shuffler.finish_into(target);
        for byte in 1..(<U as Unsigned>::bytes()) { 
            self.reshuffle(target, &|x| ((function(x).as_u64() >> (8 * byte)) % 256) as u8);
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
    fn reshuffle<F: Fn(&T)->u8>(&mut self, buffers: &mut Vec<Vec<T>>, function: &F) {
        for buffer in buffers.drain(..) {
            self.shuffler.push_batch(buffer, function);
        }
        self.shuffler.finish_into(buffers);
    }
}

struct Shuffler<T> {
    buckets: BatchedVecX256<T>,
    stash: Stash<T>
}

impl<T> Shuffler<T> {

    /// Creates a new `Shuffler` with a default capacity of `1024`.
    fn new() -> Shuffler<T> {
        Shuffler::with_capacities(lines_per_page!() * per_cache_line!(T))
    }

    /// Creates a new `Shuffler` with a specified default capacity.
    fn with_capacities(size: usize) -> Shuffler<T> {
        Shuffler {
            buckets: BatchedVecX256::new(),
            stash: Stash::new(size)
        }
    }

    /// Pushes a batch of elements into the `Shuffler` and stashes the memory backing the batch.
    #[inline]
    fn push_batch<F: Fn(&T)->u8>(&mut self, mut elements: Vec<T>, function: &F) {
        for element in elements.drain(..) {
            self.push(element, function);
        }
        self.stash.give(elements);
    }

    /// Pushes an element into the `Shuffler`, into a one of `256` arrays based on its least
    /// significant byte.
    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, function: &F) {
        let byte = function(&element);
        self.buckets.get_mut(byte as usize).push(element, &mut self.stash);
    }

    /// Finishes the shuffling into a target vector.
    fn finish_into(&mut self, target: &mut Vec<Vec<T>>) {
        for byte in 0..256 {
            self.buckets.get_mut(byte).finish_into(target);
        }
    }
}

mod test {

    #[test]
    fn test1() {

        let size = 1_000_000;

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
            sorter.push(element, &|&x| x as usize);
        }

        vector.sort();

        let mut result = Vec::new();
        for batch in sorter.finish(&|&x| x as usize) {
            result.extend(batch.into_iter());
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
