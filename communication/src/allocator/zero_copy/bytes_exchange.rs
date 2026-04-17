//! Types and traits for sharing `Bytes`.

use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use timely_bytes::arc::Bytes;
use super::bytes_slab::{BytesRefill, BytesSlab};
use super::spill::{BytesFetch, SpillPolicy};

/// An entry in a `MergeQueue`. Either `Bytes` resident in memory, or a
/// handle to bytes previously written out via a `SpillPolicy`.
pub enum QueueEntry {
    /// Bytes resident in memory, ready to be consumed directly.
    Bytes(Bytes),
    /// Bytes spilled to a backing store, fetched via the handle.
    Paged(Box<dyn BytesFetch>),
}

/// A target for `Bytes`.
pub trait BytesPush {
    /// Pushes many bytes at the instance.
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iter: I);
}
/// A source for `Bytes`.
pub trait BytesPull {
    /// Drains many bytes from the instance.
    fn drain_into(&mut self, vec: &mut Vec<Bytes>);
}

use std::sync::atomic::{AtomicBool, Ordering};
/// An unbounded queue of bytes intended for point-to-point communication between threads.
/// Writer/reader handle pairs are obtained via [`MergeQueue::new_pair`].
pub struct MergeQueue {
    queue: Arc<Mutex<VecDeque<QueueEntry>>>,    // queue of entries.
    buzzer: crate::buzzer::Buzzer,              // awakens receiver thread.
    panic: Arc<AtomicBool>,                     // used to poison the queue.
    policy: Option<Box<dyn SpillPolicy>>,       // local policy; extend or drain dispatches it.
}

impl MergeQueue {
    /// Allocates a new queue with an associated signal.
    pub fn new(buzzer: crate::buzzer::Buzzer) -> Self {
        MergeQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            buzzer,
            panic: Arc::new(AtomicBool::new(false)),
            policy: None,
        }
    }
    /// Allocates a matched pair of handles on the same underlying queue,
    /// each carrying its own policy. The first (writer) runs its policy
    /// after each `extend`; the second (reader) runs its policy before
    /// each `drain_into`.
    pub fn new_pair(
        buzzer: crate::buzzer::Buzzer,
        writer_policy: Option<Box<dyn SpillPolicy>>,
        reader_policy: Option<Box<dyn SpillPolicy>>,
    ) -> (MergeQueue, MergeQueue) {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let panic = Arc::new(AtomicBool::new(false));
        let writer = MergeQueue {
            queue: Arc::clone(&queue),
            buzzer: buzzer.clone(),
            panic: Arc::clone(&panic),
            policy: writer_policy,
        };
        let reader = MergeQueue {
            queue,
            buzzer,
            panic,
            policy: reader_policy,
        };
        (writer, reader)
    }
    /// Indicates that all input handles to the queue have dropped.
    pub fn is_complete(&self) -> bool {
        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }
        Arc::strong_count(&self.queue) == 1 && self.queue.lock().expect("Failed to acquire lock").is_empty()
    }
}

impl BytesPush for MergeQueue {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iterator: I) {

        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }

        // try to acquire lock without going to sleep (Rust's lock() might yield)
        let mut lock_ok = self.queue.try_lock();
        while let Result::Err(::std::sync::TryLockError::WouldBlock) = lock_ok {
            lock_ok = self.queue.try_lock();
        }
        let mut queue = lock_ok.expect("MergeQueue mutex poisoned.");

        let mut iterator = iterator.into_iter();
        let mut should_ping = false;
        if let Some(bytes) = iterator.next() {
            let mut tail = match queue.pop_back() {
                Some(QueueEntry::Bytes(mut tail)) => {
                    if let Err(bytes) = tail.try_merge(bytes) {
                        queue.push_back(QueueEntry::Bytes(::std::mem::replace(&mut tail, bytes)));
                    }
                    tail
                }
                Some(paged @ QueueEntry::Paged(_)) => {
                    queue.push_back(paged);
                    bytes
                }
                None => {
                    should_ping = true;
                    bytes
                }
            };

            for more_bytes in iterator {
                if let Err(more_bytes) = tail.try_merge(more_bytes) {
                    queue.push_back(QueueEntry::Bytes(::std::mem::replace(&mut tail, more_bytes)));
                }
            }
            queue.push_back(QueueEntry::Bytes(tail));
        }

        // Dispatch the spill policy, if any, while the lock is still held.
        if let Some(policy) = self.policy.as_mut() { policy.apply(&mut queue); }

        // Wakeup corresponding thread *after* releasing the lock
        ::std::mem::drop(queue);
        if should_ping {
            self.buzzer.buzz();  // only signal from empty to non-empty.
        }
    }
}

impl BytesPull for MergeQueue {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }

        // try to acquire lock without going to sleep (Rust's lock() might yield)
        let mut lock_ok = self.queue.try_lock();
        while let Result::Err(::std::sync::TryLockError::WouldBlock) = lock_ok {
            lock_ok = self.queue.try_lock();
        }
        let mut queue = lock_ok.expect("MergeQueue mutex poisoned.");

        // If a reader-side policy is installed, let it materialize Paged
        // entries near the front of the queue (up to its own budget).
        if let Some(policy) = self.policy.as_mut() { policy.apply(&mut queue); }

        // Drain Bytes entries from the front. Stop at the first Paged entry
        // (which the policy chose not to materialize) or the empty queue.
        while let Some(QueueEntry::Bytes(_)) = queue.front() {
            if let Some(QueueEntry::Bytes(b)) = queue.pop_front() {
                vec.push(b);
            }
        }

        // If we produced nothing but the queue isn't empty, something is
        // stuck (failed fetch, no reader policy, budget exhausted). Buzz
        // to ensure the consumer retries rather than parking.
        if vec.is_empty() && !queue.is_empty() {
            self.buzzer.buzz();
        }
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
        self.policy = None;
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
    fn send_buffer(&mut self) {
        let valid_len = self.buffer.valid().len();
        if valid_len > 0 {
            self.send.extend(Some(self.buffer.extract(valid_len)));
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: P, refill: BytesRefill) -> Self {
        SendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20, refill),
        }
    }
    /// Makes the next `bytes` bytes valid.
    ///
    /// The current implementation also sends the bytes, to ensure early visibility.
    pub fn make_valid(&mut self, bytes: usize) {
        self.buffer.make_valid(bytes);
        self.send_buffer();
    }
    /// Acquires a prefix of `self.empty()` of length at least `capacity`.
    pub fn reserve(&mut self, capacity: usize) -> &mut [u8] {

        if self.buffer.empty().len() < capacity {
            self.send_buffer();
            self.buffer.ensure_capacity(capacity);
        }

        assert!(self.buffer.empty().len() >= capacity);
        self.buffer.empty()
    }
    /// Marks all written data as valid, makes visible.
    pub fn publish(&mut self) {
        self.send_buffer();
    }
}

impl<P: BytesPush> Drop for SendEndpoint<P> {
    fn drop(&mut self) {
        self.send_buffer();
    }
}
