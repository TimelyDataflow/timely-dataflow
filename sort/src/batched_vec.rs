use std::{cmp, mem, ptr};
use stash::Stash;

pub struct BatchedVecRef<'a, T: 'a> {
    tail: &'a mut Vec<T>,
    batches: &'a mut Vec<Vec<T>>
}

impl<'a, T> BatchedVecRef<'a, T> {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        // It is sufficent to only check the tail for emptiness, since any time we flush
        // the tail (in .reserve), we also push some elements.
        self.tail.is_empty()
    }

    #[inline(always)]
    fn reserve(&mut self, stash: &mut Stash<T>) {
        if self.tail.len() == self.tail.capacity() {
            let complete = mem::replace(self.tail, stash.get());
            if !complete.is_empty() {
                self.batches.push(complete);
            }
        }
    }

    #[inline(always)]
    pub fn push(&mut self, element: T, stash: &mut Stash<T>) {
        self.reserve(stash);

        // The reserve call already ensured we have space. For
        // efficiency, use an unchecked push.
        unsafe {
            let len = self.tail.len();
            ptr::write(self.tail.get_unchecked_mut(len), element);
            self.tail.set_len(len + 1);
        }
    }

    pub fn push_vec(&mut self, mut elements: Vec<T>, stash: &mut Stash<T>) {
        // Fill up tail so that we keep the batches fully filled (we don't
        // want to waste memory).
        let available = self.tail.capacity() - self.tail.len();
        let to_move = cmp::min(available, elements.len());
        self.tail.extend(elements.drain(..to_move)); // This ought to compile to just 2 memcpys

        if elements.is_empty() {
            stash.give(elements);
        } else {
            let tail = mem::replace(self.tail, elements);
            if !tail.is_empty() {
                self.batches.push(tail);
            }
        }
    }

    // Note: this method is unsafe because it simply copies `elements`, and relies on the data backing it
    // to be discarded without being dropped.
    #[inline(always)]
    pub unsafe fn push_all(&mut self, elements: &[T], stash: &mut Stash<T>) {
        self.reserve(stash);

        if !(self.tail.capacity() - self.tail.len() >= elements.len()) {
            panic!("cap: {:?}, len: {:?}, pcl: {:?}", self.tail.capacity(), self.tail.len(), elements.len());
        }

        let len = self.tail.len();
        ptr::copy_nonoverlapping(elements.as_ptr(), self.tail.as_mut_ptr().offset(len as isize), elements.len());
        self.tail.set_len(len + elements.len());
    }

    #[inline(always)]
    pub fn finish_into(&mut self, target: &mut Vec<Vec<T>>) {
        target.extend(self.batches.drain(..));
        if !self.tail.is_empty() {
            target.push(mem::replace(&mut self.tail, Vec::new()));
        }
    }

    #[inline(always)]
    pub fn finish(&mut self) -> Vec<Vec<T>> {
        if !self.tail.is_empty() {
            self.batches.push(mem::replace(&mut self.tail, Vec::new()));
        }
        mem::replace(&mut self.batches, Vec::new())
    }
}

/// An efficiently-appendable collection of `T`s. Unlike `Vec<T>`, which stores all elements contiguously,
/// `BatchedVec<T>` stores elements in a bunch of independent batches, avoiding the need to periodically
/// copy lots of elements.
pub struct BatchedVec<T> {
    tail: Vec<T>,
    batches: Vec<Vec<T>>
}

impl<T> BatchedVec<T> {
    /// Create an empty `BatchedVec<T>`
    pub fn new() -> BatchedVec<T> {
        BatchedVec {
            tail: Vec::new(),
            batches: Vec::new()
        }
    }

    /// Actually access this `BatchedVec<T>`. Because of the `BatchedVecX256` optimization,
    /// we don't want to implement all the methods on `&mut BatchedVec<T>`, since `BatchedVecX256`
    /// has no way of getting those thin references. Instead, we have a custom "fat pointer" type
    /// that all access is done through, generalizing `BatchedVec` and `BatchedVecX256`.
    pub fn ref_mut(&mut self) -> BatchedVecRef<T> {
        BatchedVecRef {
            tail: &mut self.tail,
            batches: &mut self.batches
        }
    }
}

/// An optimized equivalent of `Box<[BatchedVec<T>; 256]>`. Since the tails are the most
/// commonly used parts of `BatchedVec`s, it is advantageous to store them in a separate,
/// parallel array.
pub struct BatchedVecX256<T> {
    tails: Vec<Vec<T>>,
    batches: Vec<Vec<Vec<T>>>
}

impl<T> BatchedVecX256<T> {
    /// Create a new set of 256 empty `BatchedVec`s.
    pub fn new() -> BatchedVecX256<T> {
        let mut tails = Vec::with_capacity(256);
        let mut batches = Vec::with_capacity(256);
        for _byte in 0..256 {
            tails.push(Vec::new());
            batches.push(Vec::new());
        }

        BatchedVecX256 {
            tails: tails,
            batches: batches
        }
    }

    /// Access the `BatchedVec` at the `byte` position.
    #[inline(always)]
    pub fn get_mut(&mut self, byte: usize) -> BatchedVecRef<T> {
        unsafe {
            BatchedVecRef {
                tail: self.tails.get_unchecked_mut(byte),
                batches: self.batches.get_unchecked_mut(byte)
            }
        }
    }
}
