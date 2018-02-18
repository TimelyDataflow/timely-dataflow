//! A simplified implementation of the `bytes` crate, with different features, less safety.
//!
//! #Examples
//!
//! ```
//! use bytes::rc::Bytes;
//!
//! let bytes = vec![0u8; 1024];
//! let mut shared1 = Bytes::from(bytes);
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
//! for byte in shared2.iter_mut() { *byte = 2u8; }
//! for byte in shared3.iter_mut() { *byte = 3u8; }
//! for byte in shared4.iter_mut() { *byte = 4u8; }
//!
//! drop(shared1);
//! drop(shared2);
//! drop(shared3);
//!
//! if let Ok(bytes) = shared4.try_recover() {
//!     assert_eq!(bytes[200..1024].to_vec(), [1u8;824].to_vec());
//!     assert_eq!(bytes[60..100].to_vec(),   [2u8;40].to_vec());
//!     assert_eq!(bytes[100..200].to_vec(),  [3u8;100].to_vec());
//!     assert_eq!(bytes[0..60].to_vec(),     [4u8;60].to_vec());
//! }
//! else {
//!     panic!("unrecoverable!");
//! }
//! ```
#![forbid(missing_docs)]

/// An `Rc`-backed mutable byte slice backed by a common allocation.
pub mod rc {

    use std::ops::{Deref, DerefMut};
    use std::rc::Rc;

    /// A thread-local byte buffer backed by a shared allocation.
    pub struct Bytes<B: DerefMut<Target=[u8]>> {
        /// Pointer to the start of this slice (not the allocation).
        ptr: *mut u8,
        /// Length of this slice.
        len: usize,
        /// Shared access to underlying resources.
        ///
        /// Importantly, this is unavailable for as long as the struct exists, which may
        /// prevent shared access to ptr[0 .. len]. I'm not sure I understand Rust's rules
        /// enough to make a strong statement about this.
        sequestered: Rc<B>,
    }

    impl<B: DerefMut<Target=[u8]>> Bytes<B> {

        /// Create a new instance from a byte allocation.
        pub fn from(mut bytes: B) -> Bytes<B> {
            Bytes {
                ptr: bytes.as_mut_ptr(),
                len: bytes.len(),
                sequestered: Rc::new(bytes),
            }
        }

        /// Extracts [0, index) into a new `Bytes` which is returned, updating `self`.
        ///
        /// #Safety
        ///
        /// This method uses an `unsafe` region to advance the pointer by `index`. It first
        /// tests `index` against `self.len`, which should ensure that the offset is in-bounds.
        pub fn extract_to(&mut self, index: usize) -> Bytes<B> {

            assert!(index <= self.len);

            let result = Bytes {
                ptr: self.ptr,
                len: index,
                sequestered: self.sequestered.clone(),
            };

            unsafe { self.ptr = self.ptr.offset(index as isize); }
            self.len -= index;

            result
        }

        /// Recover the underlying storage.
        ///
        /// This method either results in the underlying storage if it is uniquely held, or the
        /// input `Bytes` if it is not uniquely held.
        pub fn try_recover(self) -> Result<B, Bytes<B>> {
            match Rc::try_unwrap(self.sequestered) {
                Ok(bytes) => Ok(bytes),
                Err(rc) => Err(Bytes {
                    ptr: self.ptr,
                    len: self.len,
                    sequestered: rc,
                }),
            }
        }
    }

    impl<B: DerefMut<Target=[u8]>+Clone> Deref for Bytes<B> {
        type Target = [u8];
        fn deref(&self) -> &[u8] {
            unsafe { ::std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }

    impl<B: DerefMut<Target=[u8]>+Clone> DerefMut for Bytes<B> {
        fn deref_mut(&mut self) -> &mut [u8] {
            unsafe { ::std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }
}

/// An `Arc`-backed mutable byte slice backed by a common allocation.
pub mod arc {

    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;

    /// A thread-local byte buffer backed by a shared allocation.
    pub struct Bytes<B: DerefMut<Target=[u8]>> {
        /// Pointer to the start of this slice (not the allocation).
        ptr: *mut u8,
        /// Length of this slice.
        len: usize,
        /// Shared access to underlying resources.
        ///
        /// Importantly, this is unavailable for as long as the struct exists, which may
        /// prevent shared access to ptr[0 .. len]. I'm not sure I understand Rust's rules
        /// enough to make a strong statement about this.
        sequestered: Arc<B>,
    }

    impl<B: DerefMut<Target=[u8]>> Bytes<B> {

        /// Create a new instance from a byte allocation.
        pub fn from(mut bytes: B) -> Bytes<B> {
            Bytes {
                ptr: bytes.as_mut_ptr(),
                len: bytes.len(),
                sequestered: Arc::new(bytes),
            }
        }

        /// Extracts [0, index) into a new `Bytes` which is returned, updating `self`.
        ///
        /// #Safety
        ///
        /// This method uses an `unsafe` region to advance the pointer by `index`. It first
        /// tests `index` against `self.len`, which should ensure that the offset is in-bounds.
        pub fn extract_to(&mut self, index: usize) -> Bytes<B> {

            assert!(index <= self.len);

            let result = Bytes {
                ptr: self.ptr,
                len: index,
                sequestered: self.sequestered.clone(),
            };

            unsafe { self.ptr = self.ptr.offset(index as isize); }
            self.len -= index;

            result
        }

        /// Recover the underlying storage.
        ///
        /// This method either results in the underlying storage if it is uniquely held, or the
        /// input `Bytes` if it is not uniquely held.
        pub fn try_recover(self) -> Result<B, Bytes<B>> {
            match Arc::try_unwrap(self.sequestered) {
                Ok(bytes) => Ok(bytes),
                Err(arc) => Err(Bytes {
                    ptr: self.ptr,
                    len: self.len,
                    sequestered: arc,
                }),
            }
        }
    }

    impl<B: DerefMut<Target=[u8]>+Clone> Deref for Bytes<B> {
        type Target = [u8];
        fn deref(&self) -> &[u8] {
            unsafe { ::std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }

    impl<B: DerefMut<Target=[u8]>+Clone> DerefMut for Bytes<B> {
        fn deref_mut(&mut self) -> &mut [u8] {
            unsafe { ::std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }
}