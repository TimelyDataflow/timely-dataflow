//! A simplified implementation of the `bytes` crate, with different features, less safety.
//!
//! The crate is currently minimalist rather than maximalist, and for example does not support
//! methods on `BytesMut` that seem like they should be safe, because they are not yet needed.
//! For example, `BytesMut` should be able to implement `Send`, and `BytesMut::extract_to` could
//! return a `BytesMut` rather than a `Bytes`.
//!
//! # Examples
//!
//! ```
//! use timely_bytes::arc::BytesMut;
//!
//! let bytes = vec![0u8; 1024];
//! let mut shared1 = BytesMut::from(bytes);
//! let mut shared2 = shared1.extract_to(100);
//! let mut shared3 = shared1.extract_to(100);
//! let mut shared4 = shared2.extract_to(60);
//!
//! assert_eq!(shared1.len(), 824);
//! assert_eq!(shared2.len(), 40);
//! assert_eq!(shared3.len(), 100);
//! assert_eq!(shared4.len(), 60);
//!
//! for byte in shared1.iter_mut() { *byte = 1u8; }
//!
//! // memory in slabs [4, 2, 3, 1]: merge back in arbitrary order.
//! shared2.try_merge(shared3).ok().expect("Failed to merge 2 and 3");
//! shared2.try_merge(shared1.freeze()).ok().expect("Failed to merge 23 and 1");
//! shared4.try_merge(shared2).ok().expect("Failed to merge 4 and 231");
//!
//! assert_eq!(shared4.len(), 1024);
//! ```
#![forbid(missing_docs)]

/// An `Arc`-backed mutable byte slice backed by a common allocation.
pub mod arc {

    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use std::any::Any;

    /// A thread-safe byte buffer backed by a shared allocation.
    ///
    /// An instance of this type contends that `ptr` is valid for `len` bytes,
    /// and that no other reference to these bytes exists, other than through
    /// the type currently held in `sequestered`.
    pub struct BytesMut {
        /// Pointer to the start of this slice (not the allocation).
        ptr: *mut u8,
        /// Length of this slice.
        len: usize,
        /// Shared access to underlying resources.
        ///
        /// Importantly, this is unavailable for as long as the struct exists, which may
        /// prevent shared access to ptr[0 .. len]. I'm not sure I understand Rust's rules
        /// enough to make a stronger statement about this.
        sequestered: Arc<dyn Any>,
    }

    impl BytesMut {

        /// Create a new instance from a byte allocation.
        pub fn from<B>(bytes: B) -> BytesMut where B : DerefMut<Target=[u8]>+'static {

            // Sequester allocation behind an `Arc`, which *should* keep the address
            // stable for the lifetime of `sequestered`. The `Arc` also serves as our
            // source of truth for the allocation, which we use to re-connect slices
            // of the same allocation.
            let mut sequestered = Arc::new(bytes) as Arc<dyn Any>;
            let (ptr, len) =
            Arc::get_mut(&mut sequestered)
                .unwrap()
                .downcast_mut::<B>()
                .map(|a| (a.as_mut_ptr(), a.len()))
                .unwrap();

            BytesMut {
                ptr,
                len,
                sequestered,
            }
        }

        /// Extracts [0, index) into a new `Bytes` which is returned, updating `self`.
        ///
        /// # Safety
        ///
        /// This method first tests `index` against `self.len`, which should ensure that both
        /// the returned `Bytes` contains valid memory, and that `self` can no longer access it.
        pub fn extract_to(&mut self, index: usize) -> Bytes {

            assert!(index <= self.len);

            let result = BytesMut {
                ptr: self.ptr,
                len: index,
                sequestered: Arc::clone(&self.sequestered),
            };

            self.ptr = self.ptr.wrapping_add(index);
            self.len -= index;

            result.freeze()
        }

        /// Regenerates the BytesMut if it is uniquely held.
        ///
        /// If uniquely held, this method recovers the initial pointer and length
        /// of the sequestered allocation and re-initializes the BytesMut. The return
        /// value indicates whether this occurred.
        ///
        /// # Examples
        ///
        /// ```
        /// use timely_bytes::arc::BytesMut;
        ///
        /// let bytes = vec![0u8; 1024];
        /// let mut shared1 = BytesMut::from(bytes);
        /// let mut shared2 = shared1.extract_to(100);
        /// let mut shared3 = shared1.extract_to(100);
        /// let mut shared4 = shared2.extract_to(60);
        ///
        /// drop(shared3);
        /// drop(shared2);
        /// drop(shared4);
        /// assert!(shared1.try_regenerate::<Vec<u8>>());
        /// assert!(shared1.len() == 1024);
        /// ```
        pub fn try_regenerate<B>(&mut self) -> bool where B: DerefMut<Target=[u8]>+'static {
            // Only possible if this is the only reference to the sequestered allocation.
            if let Some(boxed) = Arc::get_mut(&mut self.sequestered) {
                let downcast = boxed.downcast_mut::<B>().expect("Downcast failed");
                self.ptr = downcast.as_mut_ptr();
                self.len = downcast.len();
                true
            }
            else {
                false
            }
        }

        /// Converts a writeable byte slice to a shareable byte slice.
        #[inline(always)]
        pub fn freeze(self) -> Bytes {
            Bytes {
                ptr: self.ptr,
                len: self.len,
                sequestered: self.sequestered,
            }
        }
    }

    impl Deref for BytesMut {
        type Target = [u8];
        #[inline(always)]
        fn deref(&self) -> &[u8] {
            unsafe { ::std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }

    impl DerefMut for BytesMut {
        #[inline(always)]
        fn deref_mut(&mut self) -> &mut [u8] {
            unsafe { ::std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }


    /// A thread-safe shared byte buffer backed by a shared allocation.
    ///
    /// An instance of this type contends that `ptr` is valid for `len` bytes,
    /// and that no other mutable reference to these bytes exists, other than
    /// through the type currently held in `sequestered`.
    #[derive(Clone)]
    pub struct Bytes {
        /// Pointer to the start of this slice (not the allocation).
        ptr: *const u8,
        /// Length of this slice.
        len: usize,
        /// Shared access to underlying resources.
        ///
        /// Importantly, this is unavailable for as long as the struct exists, which may
        /// prevent shared access to ptr[0 .. len]. I'm not sure I understand Rust's rules
        /// enough to make a stronger statement about this.
        sequestered: Arc<dyn Any>,
    }

    // Synchronization happens through `self.sequestered`, which means to ensure that even
    // across multiple threads the referenced range of bytes remain valid.
    unsafe impl Send for Bytes { }

    impl Bytes {

        /// Extracts [0, index) into a new `Bytes` which is returned, updating `self`.
        ///
        /// # Safety
        ///
        /// This method first tests `index` against `self.len`, which should ensure that both
        /// the returned `Bytes` contains valid memory, and that `self` can no longer access it.
        pub fn extract_to(&mut self, index: usize) -> Bytes {

            assert!(index <= self.len);

            let result = Bytes {
                ptr: self.ptr,
                len: index,
                sequestered: Arc::clone(&self.sequestered),
            };

            self.ptr = self.ptr.wrapping_add(index);
            self.len -= index;

            result
        }

        /// Attempts to merge adjacent slices from the same allocation.
        ///
        /// If the merge succeeds then `other.len` is added to `self` and the result is `Ok(())`.
        /// If the merge fails self is unmodified and the result is `Err(other)`, returning the
        /// bytes supplied as input.
        ///
        /// # Examples
        ///
        /// ```
        /// use timely_bytes::arc::BytesMut;
        ///
        /// let bytes = vec![0u8; 1024];
        /// let mut shared1 = BytesMut::from(bytes).freeze();
        /// let mut shared2 = shared1.extract_to(100);
        /// let mut shared3 = shared1.extract_to(100);
        /// let mut shared4 = shared2.extract_to(60);
        ///
        /// // memory in slabs [4, 2, 3, 1]: merge back in arbitrary order.
        /// shared2.try_merge(shared3).ok().expect("Failed to merge 2 and 3");
        /// shared2.try_merge(shared1).ok().expect("Failed to merge 23 and 1");
        /// shared4.try_merge(shared2).ok().expect("Failed to merge 4 and 231");
        /// ```
        pub fn try_merge(&mut self, other: Bytes) -> Result<(), Bytes> {
            if Arc::ptr_eq(&self.sequestered, &other.sequestered) && ::std::ptr::eq(self.ptr.wrapping_add(self.len), other.ptr) {
                self.len += other.len;
                Ok(())
            }
            else {
                Err(other)
            }
        }
    }

    impl Deref for Bytes {
        type Target = [u8];
        #[inline(always)]
        fn deref(&self) -> &[u8] {
            unsafe { ::std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}
