//! Types and traits for sharing `Bytes`.

use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use bytes::arc::Bytes;
use super::bytes_slab::BytesSlab;
use crate::err::CommError;

/// A target for `Bytes`.
pub trait BytesPush {
    // /// Pushes bytes at the instance.
    // fn push(&mut self, bytes: Bytes);
    /// Pushes many bytes at the instance.
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iter: I) -> crate::Result<()>;
}
/// A source for `Bytes`.
pub trait BytesPull {
    // /// Pulls bytes from the instance.
    // fn pull(&mut self) -> Option<Bytes>;
    /// Drains many bytes from the instance.
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) -> crate::Result<()>;
}

use std::sync::atomic::{AtomicBool, Ordering};
/// An unbounded queue of bytes intended for point-to-point communication
/// between threads. Cloning returns another handle to the same queue.
///
/// TODO: explain "extend"
#[derive(Clone)]
pub struct MergeQueue {
    queue: Arc<Mutex<VecDeque<Bytes>>>, // queue of bytes.
    buzzer: crate::buzzer::Buzzer,  // awakens receiver thread.
    panic: Arc<AtomicBool>,
}

impl MergeQueue {
    /// Allocates a new queue with an associated signal.
    pub fn new(buzzer: crate::buzzer::Buzzer) -> Self {
        MergeQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            buzzer,
            panic: Arc::new(AtomicBool::new(false)),
        }
    }
    /// Indicates that all input handles to the queue have dropped.
    pub fn is_complete(&self) -> crate::Result<bool> {
        if self.panic.load(Ordering::SeqCst) { return Err(CommError::Poison); }
        Ok(Arc::strong_count(&self.queue) == 1 && self.queue.lock()?.is_empty())
    }

    /// Mark self as poisoned, which causes all subsequent operations to error.
    pub fn poison(&mut self) {
        self.panic.store(true, Ordering::SeqCst);
    }
}

impl BytesPush for MergeQueue {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iterator: I) -> crate::Result<()> {

        if self.panic.load(Ordering::SeqCst) { return Err(CommError::Poison); }

        // try to acquire lock without going to sleep (Rust's lock() might yield)
        let mut lock_ok = self.queue.try_lock();
        while let Err(::std::sync::TryLockError::WouldBlock) = lock_ok {
            lock_ok = self.queue.try_lock();
        }
        let mut queue = lock_ok?;

        let mut iterator = iterator.into_iter();
        let mut should_ping = false;
        if let Some(bytes) = iterator.next() {
            let mut tail = if let Some(mut tail) = queue.pop_back() {
                if let Err(bytes) = tail.try_merge(bytes) {
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
                tail
            }
            else {
                should_ping = true;
                bytes
            };

            for bytes in iterator {
                if let Err(bytes) = tail.try_merge(bytes) {
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
            }
            queue.push_back(tail);
        }

        // Wakeup corresponding thread *after* releasing the lock
        ::std::mem::drop(queue);
        if should_ping {
            self.buzzer.buzz();  // only signal from empty to non-empty.
        }
        Ok(())
    }
}

impl BytesPull for MergeQueue {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) -> crate::Result<()> {
        if self.panic.load(Ordering::SeqCst) { return Err(CommError::Poison); }

        // try to acquire lock without going to sleep (Rust's lock() might yield)
        let mut lock_ok = self.queue.try_lock();
        while let Err(::std::sync::TryLockError::WouldBlock) = lock_ok {
            lock_ok = self.queue.try_lock();
        }
        let mut queue = lock_ok?;

        vec.extend(queue.drain(..));
        Ok(())
    }
}

// We want to ping in the drop because a channel closing can unblock a thread waiting on
// the next bit of data to show up.
impl Drop for MergeQueue {
    fn drop(&mut self) {
        // Propagate panic information, to distinguish between clean and unclean shutdown.
        if ::std::thread::panicking() {
            self.panic.store(true, Ordering::SeqCst);
        }
        else {
            // TODO: Perhaps this aggressive ordering can relax orderings elsewhere.
            if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }
        }
        // Drop the queue before pinging.
        self.queue = Arc::new(Mutex::new(VecDeque::new()));
        self.buzzer.buzz();
    }
}


/// A `BytesPush` wrapper which stages writes.
pub struct SendEndpoint<P: BytesPush> {
    send: P,
    buffer: BytesSlab,
}

impl<P: BytesPush> SendEndpoint<P> {

    /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
    fn send_buffer(&mut self) -> crate::Result<()> {
        let valid_len = self.buffer.valid().len();
        if valid_len > 0 {
            self.send.extend(Some(self.buffer.extract(valid_len)))?;
        }
        Ok(())
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: P) -> Self {
        SendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20),
        }
    }
    /// Makes the next `bytes` bytes valid.
    ///
    /// The current implementation also sends the bytes, to ensure early visibility.
    pub fn make_valid(&mut self, bytes: usize) -> crate::Result<()> {
        self.buffer.make_valid(bytes);
        self.send_buffer()
    }
    /// Acquires a prefix of `self.empty()` of length at least `capacity`.
    pub fn reserve(&mut self, capacity: usize) -> crate::Result<&mut [u8]> {

        if self.buffer.empty().len() < capacity {
            self.send_buffer()?;
            self.buffer.ensure_capacity(capacity);
        }

        assert!(self.buffer.empty().len() >= capacity);
        Ok(self.buffer.empty())
    }
    /// Marks all written data as valid, makes visible.
    pub fn publish(&mut self) -> crate::Result<()> {
        self.send_buffer()
    }
}

impl<P: BytesPush> Drop for SendEndpoint<P> {
    fn drop(&mut self) {
        // Ignore errors.
        let _ = self.send_buffer();
    }
}
