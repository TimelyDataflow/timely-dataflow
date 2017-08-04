use std::slice;

use stash::Stash;
use batched_vec::BatchedVecRef;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

pub struct SWCBuffer<T> {
    staged: Vec<T>,
    counts: [u8; 256],
}

impl<T> SWCBuffer<T> {

    pub fn new() -> Self {
        let staged = Vec::with_capacity(256 * per_cache_line!(T));
        let addr: usize = unsafe { ::std::mem::transmute(staged.as_ptr()) };

        // looks like this is often cache-line aligned; yay!
        assert_eq!(addr % 64, 0);

        SWCBuffer {
            staged: staged,
            counts: [0u8; 256],
        }
    }

    #[inline(always)]
    pub fn slice(&self, byte: usize) -> &[T] {
        unsafe {
            slice::from_raw_parts(
                self.staged.as_ptr().offset(per_cache_line!(T) as isize * byte as isize),
                *self.counts.get_unchecked(byte) as usize
            )
        }
    }
    
    #[inline(always)]
    pub fn full(&self, byte: usize) -> bool {
        unsafe { (*self.counts.get_unchecked(byte) as usize) == per_cache_line!(T) } 
    }

    #[inline(always)]
    pub fn count(&self, byte: usize) -> usize {
        unsafe { *self.counts.get_unchecked(byte) as usize } 
    }

    #[inline(always)]
    pub fn push(&mut self, element: T, byte: usize) {
        unsafe {
            let offset = per_cache_line!(T) as isize * byte as isize + *self.counts.get_unchecked(byte as usize) as isize;
            ::std::ptr::write(self.staged.as_mut_ptr().offset(offset), element);
            *self.counts.get_unchecked_mut(byte) += 1;
        }
    }

    #[inline(always)]
    pub fn drain_into<'a>(&mut self, byte: usize, batch: &mut BatchedVecRef<'a, T>, stash: &mut Stash<T>) where T: 'a {
        unsafe {
            if *self.counts.get_unchecked(byte) > 0 {
                batch.push_all(self.slice(byte), stash);
                *self.counts.get_unchecked_mut(byte) = 0;
            }
        }
    }

    #[inline(always)]
    pub fn drain_into_vec(&mut self, byte: usize, batch: &mut Vec<T>, _stash: &mut Stash<T>) {
        unsafe {
            for i in 0 .. *self.counts.get_unchecked(byte) {
                batch.push(::std::ptr::read(self.staged.as_mut_ptr().offset(per_cache_line!(T) as isize * byte as isize + i as isize)));
            }
            *self.counts.get_unchecked_mut(byte) = 0;
        }
    }    
}