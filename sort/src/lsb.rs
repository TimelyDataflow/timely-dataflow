use ::Unsigned;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 2 * 4096 / 64 }}
}

pub struct RadixSorter<T> {
    shuffler: RadixShuffler<T>,
}

impl<T> RadixSorter<T> {
    pub fn new() -> RadixSorter<T> {
        RadixSorter {
            shuffler: RadixShuffler::new(),
        }
    }
    #[inline]
    pub fn extend<U: Unsigned, F: Fn(&T)->U, I: Iterator<Item=T>>(&mut self, iterator: I, function: &F) {
        for element in iterator {
            self.push(element, function);
        }
    }
    #[inline]
    pub fn push<U: Unsigned, F: Fn(&T)->U>(&mut self, element: T, function: &F) {
        self.shuffler.push(element, &|x| (function(x).as_u64() % 256) as u8);
    }
    #[inline]
    pub fn push_batch<U: Unsigned, F: Fn(&T)->U>(&mut self, batch: Vec<T>, function: &F) {
        self.shuffler.push_batch(batch,  &|x| (function(x).as_u64() % 256) as u8);
    }
    pub fn sort<U: Unsigned, F: Fn(&T)->U>(&mut self, batches: &mut Vec<Vec<T>>, function: &F) {
        for batch in batches.drain(..) { self.push_batch(batch, function); }
        *batches = self.finish(function);
    }

    pub fn finish<U: Unsigned, F: Fn(&T)->U>(&mut self, function: &F) -> Vec<Vec<T>> {
        let mut sorted = self.shuffler.finish();
        for byte in 1..(<U as Unsigned>::bytes()) { 
            sorted = self.reshuffle(sorted, &|x| ((function(x).as_u64() >> (8 * byte)) % 256) as u8);
        }
        sorted
    }
    pub fn recycle(&mut self, mut buffers: Vec<Vec<T>>) {
        for mut buffer in buffers.drain(..) {
            buffer.clear();
            self.shuffler.push_batch(buffer, &|_| 0);
        }
    }
    #[inline(always)]
    fn reshuffle<F: Fn(&T)->u8>(&mut self, buffers: Vec<Vec<T>>, function: &F) -> Vec<Vec<T>> {
        for buffer in buffers.into_iter() {
            self.shuffler.push_batch(buffer, function);
        }
        self.shuffler.finish()
    }
}

///
struct RadixShuffler<T> {
    fronts: Vec<Vec<T>>,
    buffers: Vec<Vec<Vec<T>>>, // for each byte, a list of segments
    stashed: Vec<Vec<T>>,      // spare segments
    default_capacity: usize,
}

impl<T> RadixShuffler<T> {

    /// Creates a new `RadixShuffler` with a default capacity of `1024`.
    fn new() -> RadixShuffler<T> {
        RadixShuffler::with_capacities(lines_per_page!() * per_cache_line!(T))
    }

    /// Creates a new `RadixShuffler` with a specified default capacity.
    fn with_capacities(size: usize) -> RadixShuffler<T> {
        let mut buffers = vec![]; for _ in 0..256 { buffers.push(Vec::new()); }
        let mut fronts = vec![]; for _ in 0..256 { fronts.push(Vec::new()); }

        RadixShuffler {
            buffers: buffers,
            stashed: vec![],
            fronts: fronts,
            default_capacity: size,
        }
    }

    /// Pushes a batch of elements into the `RadixShuffler` and stashes the memory backing the batch.
    #[inline]
    fn push_batch<F: Fn(&T)->u8>(&mut self, mut elements: Vec<T>, function: &F) {
        for element in elements.drain(..) {
            self.push(element, function);
        }
        // TODO : determine some discipline for when to keep buffers vs not.
        // if elements.capacity() == self.default_capacity {
            self.stashed.push(elements);
        // }
    }

    /// Pushes an element into the `RadixShuffler`, into a one of `256` arrays based on its least
    /// significant byte.
    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, function: &F) {

        let byte = function(&element) as usize;

        // write the element to our scratch buffer space and consider it taken care of.
        // test the buffer capacity first, so that we can leave them uninitialized.
        unsafe {
            if self.fronts.get_unchecked(byte).len() == self.fronts.get_unchecked(byte).capacity() {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(self.default_capacity));
                let complete = ::std::mem::replace(&mut self.fronts[byte], replacement);
                if complete.len() > 0 {
                    self.buffers[byte].push(complete);
                }
            }

            let len = self.fronts.get_unchecked(byte).len();
            ::std::ptr::write((*self.fronts.get_unchecked_mut(byte)).get_unchecked_mut(len), element);
            self.fronts.get_unchecked_mut(byte).set_len(len + 1);
        }
    }

    /// Finishes the shuffling, returning a sequence of elements as a vector of buffers.
    fn finish(&mut self) -> Vec<Vec<T>> {

        for byte in 0..256 {
            if self.fronts[byte].len() > 0 {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(1024));
                let complete = ::std::mem::replace(&mut self.fronts[byte], replacement);
                self.buffers[byte].push(complete);
            }
        }

        let mut result = vec![];
        for byte in 0..256 {
            result.extend(self.buffers[byte].drain(..));
        }
        result
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
