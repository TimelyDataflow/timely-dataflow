//! A large binary allocation for writing and sharing.

use std::ops::{Deref, DerefMut};
use timely_bytes::arc::{Bytes, BytesMut};

/// A large binary allocation for writing and sharing.
///
/// A bytes slab wraps a `BytesMut` and maintains a valid (written) length, and supports writing after
/// this valid length, and extracting `Bytes` up to this valid length. Extracted bytes are enqueued
/// and checked for uniqueness in order to recycle them (once all shared references are dropped).
pub struct BytesSlab {
    buffer:         BytesMut,                   // current working buffer.
    in_progress:    Vec<Option<BytesMut>>,      // buffers shared with workers.
    stash:          Vec<BytesMut>,              // reclaimed and reusable buffers.
    shift:          usize,                      // current buffer allocation size.
    valid:          usize,                      // buffer[..valid] are valid bytes.
    new_bytes:      BytesRefill,                // function to allocate new buffers.
}

/// Ability to acquire and policy to retain byte buffers.
#[derive(Clone)]
pub struct BytesRefill {
    /// Logic to acquire a new buffer of a certain number of bytes.
    pub logic: std::sync::Arc<dyn Fn(usize) -> Box<dyn DerefMut<Target=[u8]>>+Send+Sync>,
    /// An optional limit on the number of empty buffers retained.
    pub limit: Option<usize>,
}

impl BytesSlab {
    /// Allocates a new `BytesSlab` with an initial size determined by a shift.
    pub fn new(shift: usize, new_bytes: BytesRefill) -> Self {
        BytesSlab {
            buffer: BytesMut::from(BoxDerefMut { boxed: (new_bytes.logic)(1 << shift) }),
            in_progress: Vec::new(),
            stash: Vec::new(),
            shift,
            valid: 0,
            new_bytes,
        }
    }
    /// The empty region of the slab.
    pub fn empty(&mut self) -> &mut [u8] {
        &mut self.buffer[self.valid..]
    }
    /// The valid region of the slab.
    pub fn valid(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.valid]
    }
    /// Marks the next `bytes` bytes as valid.
    pub fn make_valid(&mut self, bytes: usize) {
        self.valid += bytes;
    }
    /// Extracts the first `bytes` valid bytes.
    pub fn extract(&mut self, bytes: usize) -> Bytes {
        debug_assert!(bytes <= self.valid);
        self.valid -= bytes;
        self.buffer.extract_to(bytes)
    }

    /// Ensures that `self.empty().len()` is at least `capacity`.
    ///
    /// This method may retire the current buffer if it does not have enough space, in which case
    /// it will copy any remaining contents into a new buffer. If this would not create enough free
    /// space, the shift is increased until it is sufficient.
    pub fn ensure_capacity(&mut self, capacity: usize) {

        if self.empty().len() < capacity {

            let mut increased_shift = false;

            // Increase allocation if copy would be insufficient.
            while self.valid + capacity > (1 << self.shift) {
                self.shift += 1;
                self.stash.clear();         // clear wrongly sized buffers.
                self.in_progress.clear();   // clear wrongly sized buffers.
                increased_shift = true;
            }

            // Attempt to reclaim shared slices.
            if self.stash.is_empty() {
                for shared in self.in_progress.iter_mut() {
                    if let Some(mut bytes) = shared.take() {
                        if bytes.try_regenerate::<BoxDerefMut>() {
                            // NOTE: Test should be redundant, but better safe...
                            if bytes.len() == (1 << self.shift) {
                                self.stash.push(bytes);
                            }
                        }
                        else {
                            *shared = Some(bytes);
                        }
                    }
                }
                self.in_progress.retain(|x| x.is_some());
            }

            let new_buffer = self.stash.pop().unwrap_or_else(|| BytesMut::from(BoxDerefMut { boxed: (self.new_bytes.logic)(1 << self.shift) }));
            let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

            if let Some(limit) = self.new_bytes.limit {
                self.stash.truncate(limit);
            }

            self.buffer[.. self.valid].copy_from_slice(&old_buffer[.. self.valid]);
            if !increased_shift {
                self.in_progress.push(Some(old_buffer));
            }
        }
    }
}

/// A wrapper for `Box<dyn DerefMut<Target=T>>` that dereferences to `T` rather than `dyn DerefMut<Target=T>`.
struct BoxDerefMut {
    boxed: Box<dyn DerefMut<Target=[u8]>+'static>,
}

impl Deref for BoxDerefMut {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.boxed[..]
    }
}

impl DerefMut for BoxDerefMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.boxed[..]
    }
}
