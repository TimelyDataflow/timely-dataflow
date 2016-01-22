use ::Unsigned;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 2 * 4096 / 64 }}
}

pub struct RadixSorter<T> {
    shuffler: RShuffle<T>,
}

impl<T> RadixSorter<T> {
    pub fn new() -> RadixSorter<T> {
        RadixSorter {
            shuffler: RShuffle::new(),
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

// The following is a software write-combining implementation. This technique is meant to cut
// down on the the amount of memory traffic due to non-full cache lines moving around. at the
// moment, on my laptop, it doesn't seem to improve things. But! I hope that in the fullness of
// time this changes, either because the implementation improves, or I get a laptop with seventy
// bazillion cores.

pub struct RShuffle<T> {
    staged: Vec<T>,     // ideally: 256 * number of T element per cache line.
    counts: [u8; 256],

    fronts: Vec<Vec<T>>,
    buffers: Vec<Vec<Vec<T>>>, // for each byte, a list of segments
    stashed: Vec<Vec<T>>,      // spare segments
}

impl<T> RShuffle<T> {
    fn new() -> RShuffle<T> {
        let mut buffers = vec![]; for _ in 0..256 { buffers.push(Vec::new()); }
        let mut fronts = vec![]; for _ in 0..256 { fronts.push(Vec::with_capacity(lines_per_page!() * per_cache_line!(T))); }

        let staged = Vec::with_capacity(256 * per_cache_line!(T));

        // looks like this is often cache-line aligned; yay!
        let addr: usize = unsafe { ::std::mem::transmute(staged.as_ptr()) };
        assert_eq!(addr % 64, 0);

        RShuffle {
            staged: staged,
            counts: [0; 256],
            buffers: buffers,
            stashed: vec![],
            fronts: fronts,
        }
    }
    fn push_batch<F: Fn(&T)->u8>(&mut self, mut elements: Vec<T>, function: &F) {
        for element in elements.drain(..) {
            self.push(element, function);
        }

        self.stashed.push(elements);
    }
    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, function: &F) {

        let byte = function(&element) as usize;

        // write the element to our scratch buffer space and consider it taken care of.
        unsafe {

            // self.staged[byte * stride + self.counts[byte]] = element; self.counts[byte] += 1
            ::std::ptr::write(self.staged.as_mut_ptr().offset(per_cache_line!(T) as isize * byte as isize + *self.counts.get_unchecked(byte) as isize), element);
            *self.counts.get_unchecked_mut(byte) += 1;

            // if we have saturated the buffer for byte, flush it out
            if *self.counts.get_unchecked(byte) as usize == per_cache_line!(T) {

                // the position we will write to
                let front_len = self.fronts.get_unchecked(byte).len();
                ::std::ptr::copy_nonoverlapping(self.staged.as_ptr().offset(per_cache_line!(T) as isize * byte as isize),
                                                self.fronts[byte].as_mut_ptr().offset(front_len as isize),
                                                per_cache_line!(T));

                self.fronts.get_unchecked_mut(byte).set_len(front_len + per_cache_line!(T));

                // assert!(self.fronts.get_unchecked(byte).capacity() == lines_per_page!() * per_cache_line!(T));
                if self.fronts.get_unchecked(byte).len() == self.fronts.get_unchecked(byte).capacity() {
                    let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(lines_per_page!() * per_cache_line!(T)));
                    let complete = ::std::mem::replace(&mut self.fronts[byte], replacement);
                    self.buffers[byte].push(complete);
                }
                self.counts[byte] = 0;
            }
        }
    }
    fn finish(&mut self) -> Vec<Vec<T>> {

        for byte in 0..256 {
            for i in 0..self.counts[byte] {
                unsafe {
                    self.fronts[byte].push(::std::ptr::read(self.staged.as_mut_ptr().offset(per_cache_line!(T) as isize * byte as isize + i as isize)));
                }
            }
            self.counts[byte] = 0;

            if self.fronts[byte].len() > 0 {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(lines_per_page!() * per_cache_line!(T)));
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