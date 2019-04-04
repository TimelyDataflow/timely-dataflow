use std::mem::size_of;
use std::slice;

use crate::stash::Stash;
use crate::batched_vec::BatchedVecRef;

fn per_cache_line<T>() -> usize {
    std::cmp::max(64 / size_of::<T>(), 4)
}

#[repr(align(64))]
pub struct CacheLine([u8; 64]);

pub struct SWCBuffer<T> {
    counts: [u8; 256],
    staged: Vec<CacheLine>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> SWCBuffer<T> {
    pub fn new() -> Self {
        let nlines = (256 * per_cache_line::<T>() * size_of::<T>()) / 64;
        let staged = Vec::<CacheLine>::with_capacity(nlines);

        // verify cache alignment
        let addr = staged.as_ptr() as usize;
        assert_eq!(addr % 64, 0);

        SWCBuffer {
            staged: staged,
            counts: [0u8; 256],
            phantom: std::marker::PhantomData,
        }
    }

    fn staged_ptr(&self) -> *const T {
        self.staged.as_ptr() as _
    }

    fn staged_mut_ptr(&mut self) -> *mut T {
        self.staged.as_mut_ptr() as _
    }

    #[inline]
    pub fn slice(&self, byte: usize) -> &[T] {
        unsafe {
            slice::from_raw_parts(
                self.staged_ptr().offset(per_cache_line::<T>() as isize * byte as isize),
                *self.counts.get_unchecked(byte) as usize
            )
        }
    }

    #[inline]
    pub fn full(&self, byte: usize) -> bool {
        unsafe { (*self.counts.get_unchecked(byte) as usize) == per_cache_line::<T>() }
    }

    #[inline]
    pub fn count(&self, byte: usize) -> usize {
        unsafe { *self.counts.get_unchecked(byte) as usize }
    }

    #[inline]
    pub fn push(&mut self, element: T, byte: usize) {
        unsafe {
            let offset = per_cache_line::<T>() as isize * byte as isize + *self.counts.get_unchecked(byte as usize) as isize;
            std::ptr::write(self.staged_mut_ptr().offset(offset), element);
            *self.counts.get_unchecked_mut(byte) += 1;
        }
    }

    #[inline]
    pub fn drain_into<'a>(&mut self, byte: usize, batch: &mut BatchedVecRef<'a, T>, stash: &mut Stash<T>) where T: 'a {
        unsafe {
            if *self.counts.get_unchecked(byte) > 0 {
                batch.push_all(self.slice(byte), stash);
                *self.counts.get_unchecked_mut(byte) = 0;
            }
        }
    }

    #[inline]
    pub fn drain_into_vec(&mut self, byte: usize, batch: &mut Vec<T>, _stash: &mut Stash<T>) {
        unsafe {
            for i in 0 .. *self.counts.get_unchecked(byte) {
                batch.push(std::ptr::read(self.staged_mut_ptr().offset(per_cache_line::<T>() as isize * byte as isize + i as isize)));
            }
            *self.counts.get_unchecked_mut(byte) = 0;
        }
    }
}
