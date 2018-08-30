//! Types and traits for sharing `Bytes`.

use bytes::arc::Bytes;
use super::bytes_slab::BytesSlab;

/// A target for `Bytes`.
pub trait BytesPush {
    // /// Pushes bytes at the instance.
    // fn push(&mut self, bytes: Bytes);
    /// Pushes many bytes at the instance.
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iter: I);
}
/// A source for `Bytes`.
pub trait BytesPull {
    // /// Pulls bytes from the instance.
    // fn pull(&mut self) -> Option<Bytes>;
    /// Drains many bytes from the instance.
    fn drain_into(&mut self, vec: &mut Vec<Bytes>);
}

// std::sync::mpsc implementations.
use ::std::sync::mpsc::{Sender, Receiver};
impl BytesPush for Sender<Bytes> {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iterator: I) {
        for bytes in iterator {
            self.send(bytes)
                .expect("unable to send Bytes");
        }
    }
}

impl BytesPull for Receiver<Bytes> {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        while let Some(item) = self.try_recv().ok() {
            vec.push(item);
        }
    }
}

// Arc<Mutex<VecDeque<Bytes>>> implementations.
use ::std::sync::{Arc, Mutex, Condvar};
use ::std::collections::VecDeque;
impl BytesPush for Arc<Mutex<VecDeque<Bytes>>> {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iter: I) {
        self.lock()
            .expect("unable to lock mutex")
            .extend(iter);
    }
}

impl BytesPull for Arc<Mutex<VecDeque<Bytes>>> {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        let mut queue = self.lock().expect("unable to lock mutex");
        vec.extend(queue.drain(..));
    }
}

use std::sync::RwLock;
use std::thread::Thread;
/// A signal appropriate to wake a single thread.
///
/// Internally this type uses thread parking and unparking, where the first thread to call
/// `wait` is registered as the thread to wake. Other threads that call `wait` will just be
/// parked without registering themselves, which would probably be a bug (of theirs).
#[derive(Clone)]
pub struct Signal {
    thread: Arc<RwLock<Option<Thread>>>,
}

impl Signal {
    /// Creates a new signal.
    pub fn new() -> Self {
        Signal { thread: Arc::new(RwLock::new(None)) }
    }
    /// Blocks unless or until ping is called.
    pub fn wait(&self) {
        // It is important not to block on the first call; doing so would fail to unblock
        // from pings before the first call to wait. This may appear as a spurious wake-up,
        // and ideally the caller is prepared for that.
        if self.thread.read().expect("failed to read thread").is_none() {
            *self.thread.write().expect("failed to set thread") = Some(::std::thread::current())
        }
        else {
            ::std::thread::park();
        }
    }
    /// Unblocks the current or next call to wait.
    pub fn ping(&self) {
        if let Some(thread) = self.thread.read().expect("failed to read thread").as_ref() {
            thread.unpark();
        }
    }
}

// /// A signal which
// #[derive(Clone)]
// pub struct Signal {
//     thing: Arc<(Mutex<bool>, Condvar)>,
// }

// impl Signal {
//     /// Allocates a new signal.
//     pub fn new() -> Self {
//         Signal { thing: Arc::new((Mutex::new(false), Condvar::new())) }
//     }
//     /// Blocks until the signal is set, and then unsets the signal.
//     pub fn wait(&self) {
//         let mut signaled = self.thing.0.lock().expect("Failed to lock mutex.");
//         while !*signaled {
//             signaled = self.thing.1.wait(signaled).expect("Failed to wait.");
//         }
//         *signaled = false;
//     }
//     /// Sets the signal, unblocking a waiting thread.
//     pub fn ping(&self) {
//         *self.thing.0.lock().expect("Failed to lock mutex.") = true;
//         self.thing.1.notify_one();
//     }
// }

/// Who knows.
#[derive(Clone)]
pub struct MergeQueue {
    queue: Arc<Mutex<VecDeque<Bytes>>>, // queue of bytes.
    dirty: Signal,                      // indicates whether there may be data present.
}

impl MergeQueue {
    /// Allocates a new queue with an associated signal.
    pub fn new(signal: Signal) -> Self {
        MergeQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            dirty: signal,
        }
    }
    /// Indicates that all input handles to the queue have dropped.
    pub fn is_complete(&self) -> bool {
        Arc::strong_count(&self.queue) == 1
    }
}

impl BytesPush for MergeQueue {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iterator: I) {
        // should lock once, extend; shouldn't re-lock.
        let mut queue = self.queue.lock().expect("Failed to lock queue");
        let mut iterator = iterator.into_iter();
        if let Some(bytes) = iterator.next() {
            let mut tail = if let Some(mut tail) = queue.pop_back() {
                if let Err(bytes) = tail.try_merge(bytes) {
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
                tail
            }
            else {
                self.dirty.ping();  // only signal from empty to non-empty.
                bytes
            };

            for bytes in iterator {
                if let Err(bytes) = tail.try_merge(bytes) {
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
            }
            queue.push_back(tail);
        }
    }
}

impl BytesPull for MergeQueue {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        let mut queue = self.queue.lock().expect("unable to lock mutex");
        vec.extend(queue.drain(..));
    }
}

// We want to ping in the drop because a channel closing can unblock a thread waiting on
// the next bit of data to show up.
impl Drop for MergeQueue {
    fn drop(&mut self) {
        // Drop the queue before pinging.
        self.queue = Arc::new(Mutex::new(VecDeque::new()));
        self.dirty.ping();
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
    pub fn new(queue: P) -> Self {
        SendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20),
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

