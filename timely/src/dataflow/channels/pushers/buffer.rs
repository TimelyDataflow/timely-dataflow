//! Buffering and session mechanisms to provide the appearance of record-at-a-time sending,
//! with the performance of batched sends.

use crate::dataflow::channels::{BundleCore, Message};
use crate::progress::Timestamp;
use crate::dataflow::operators::Capability;
use crate::communication::{Push, Container};
use crate::Data;
use crate::communication::message::RefOrMut;

/// Buffers data sent at the same time, for efficient communication.
///
/// The `Buffer` type should be used by calling `session` with a time, which checks whether
/// data must be flushed and creates a `Session` object which allows sending at the given time.
pub struct BufferCore<T, D: Container, P: Push<BundleCore<T, D>>> {
    time: Option<T>,  // the currently open time, if it is open
    buffer: Option<D>,   // a buffer for records, to send at self.time
    /// An allocation to reconstruct buffers
    allocation: Option<D::Allocation>,
    pusher: P,
}

/// A buffer specialized to vector-based containers.
pub type Buffer<T, D, P> = BufferCore<T, Vec<D>, P>;

// Cannot derive `Debug` for [Buffer] because we cannot express the constraint that
// `C::Builder: Debug`.
impl<T: ::std::fmt::Debug, C: Container, P: Push<BundleCore<T, C>>+::std::fmt::Debug> ::std::fmt::Debug for BufferCore<T, C, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("BufferCore");
        debug.field("time", &self.time);
        debug.field("pusher", &self.pusher);
        debug.field("buffer", &"...");
        debug.field("allocation", &"...");
        debug.finish()
    }
}

impl<T, C: Container, P: Push<BundleCore<T, C>>> BufferCore<T, C, P> where T: Eq+Clone {

    /// Creates a new `Buffer`.
    pub fn new(pusher: P) -> Self {
        Self {
            time: None,
            buffer: None,
            allocation: None,
            pusher,
        }
    }

    /// Returns a `Session`, which accepts data to send at the associated time
    pub fn session(&mut self, time: &T) -> Session<T, C, P> {
        if let Some(true) = self.time.as_ref().map(|x| x != time) { self.flush(); }
        self.time = Some(time.clone());
        Session { buffer: self }
    }
    /// Allocates a new `AutoflushSession` which flushes itself on drop.
    pub fn autoflush_session(&mut self, cap: Capability<T>) -> AutoflushSessionCore<T, C, P> where T: Timestamp {
        if let Some(true) = self.time.as_ref().map(|x| x != cap.time()) { self.flush(); }
        self.time = Some(cap.time().clone());
        AutoflushSessionCore {
            buffer: self,
            _capability: cap,
        }
    }

    /// Returns a reference to the inner `P: Push` type.
    ///
    /// This is currently used internally, and should not be used without some care.
    pub fn inner(&mut self) -> &mut P { &mut self.pusher }

    /// Flushes all data and pushes a `None` to `self.pusher`, indicating a flush.
    pub fn cease(&mut self) {
        self.flush();
        self.pusher.push(None, &mut None);
    }

    /// moves the contents of
    fn flush(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let time = self.time.as_ref().unwrap().clone();
            Message::push_at(Some(buffer), time, &mut self.pusher, &mut self.allocation);
        }
    }

    // Gives an entire container at a specific time.
    fn give_container(&mut self, vector: C, allocation: &mut Option<C::Allocation>) {
        // flush to ensure fifo-ness
        self.flush();

        let time = self.time.as_ref().expect("Buffer::give_vec(): time is None.").clone();
        Message::push_at(Some(vector), time, &mut self.pusher, allocation);
    }
}

impl<T, D: Data, P: Push<BundleCore<T, Vec<D>>>> BufferCore<T, Vec<D>, P> where T: Eq+Clone {
    fn ensure_buffer(&mut self) -> &mut Vec<D> {
        if self.buffer.is_none() {
            self.buffer = Some(RefOrMut::Ref(&Vec::new()).assemble(&mut self.allocation));
        }
        self.buffer.as_mut().unwrap()
    }

    // internal method for use by `Session`.
    fn give(&mut self, data: D) {
        let buffer = self.ensure_buffer();
        buffer.push(data);
        // assert!(self.buffer.capacity() == Message::<O::Data>::default_length());
        if buffer.len() == buffer.capacity() {
            self.flush();
        }
    }

    // Gives an entire message at a specific time.
    fn give_vec(&mut self, vector: &mut Vec<D>) {
        // flush to ensure fifo-ness
        self.flush();

        let time = self.time.as_ref().expect("Buffer::give_vec(): time is None.").clone();
        let mut allocation = None;
        let data = ::std::mem::take(vector);
        Message::push_at(Some(data), time, &mut self.pusher, &mut allocation);
        if let Some(allocation) = allocation {
            *vector = allocation;
        }
    }
}


/// An output session for sending records at a specified time.
///
/// The `Session` struct provides the user-facing interface to an operator output, namely
/// the `Buffer` type. A `Session` wraps a session of output at a specified time, and
/// avoids what would otherwise be a constant cost of checking timestamp equality.
pub struct Session<'a, T, C: Container, P: Push<BundleCore<T, C>>+'a> where T: Eq+Clone+'a, C: 'a {
    buffer: &'a mut BufferCore<T, C, P>,
}

impl<'a, T, C: Container, P: Push<BundleCore<T, C>>+'a> Session<'a, T, C, P>  where T: Eq+Clone+'a, C: 'a {
    /// Provide a container at the time specified by the [Session].
    pub fn give_container(&mut self, container: C, allocation: &mut Option<C::Allocation>) {
        self.buffer.give_container(container, allocation)
    }
}

impl<'a, T, D: Data, P: Push<BundleCore<T, Vec<D>>>+'a> Session<'a, T, Vec<D>, P>  where T: Eq+Clone+'a, D: 'a {
    /// Provides one record at the time specified by the `Session`.
    #[inline]
    pub fn give(&mut self, data: D) {
        self.buffer.give(data);
    }
    /// Provides an iterator of records at the time specified by the `Session`.
    #[inline]
    pub fn give_iterator<I: Iterator<Item=D>>(&mut self, iter: I) {
        for item in iter {
            self.give(item);
        }
    }
    /// Provides a fully formed `Content<D>` message for senders which can use this type.
    ///
    /// The `Content` type is the backing memory for communication in timely, and it can
    /// often be more efficient to reuse this memory rather than have timely allocate
    /// new backing memory.
    #[inline]
    pub fn give_vec(&mut self, message: &mut Vec<D>) {
        if !message.is_empty() {
            self.buffer.give_vec(message);
        }
    }
}

/// A session which will flush itself when dropped.
pub struct AutoflushSessionCore<'a, T: Timestamp, C: Container, P: Push<BundleCore<T, C>>+'a> where
    T: Eq+Clone+'a, C: 'a {
    /// A reference to the underlying buffer.
    buffer: &'a mut BufferCore<T, C, P>,
    /// The capability being used to send the data.
    _capability: Capability<T>,
}

/// Auto-flush session specialized to vector-based containers.
pub type AutoFlushSession<'a, T, D, P> = AutoflushSessionCore<'a, T, Vec<D>, P>;

impl<'a, T: Timestamp, D: Data, P: Push<BundleCore<T, Vec<D>>>+'a> AutoflushSessionCore<'a, T, Vec<D>, P> where T: Eq+Clone+'a, D: 'a {
    /// Transmits a single record.
    #[inline]
    pub fn give(&mut self, data: D) {
        self.buffer.give(data);
    }
    /// Transmits records produced by an iterator.
    #[inline]
    pub fn give_iterator<I: Iterator<Item=D>>(&mut self, iter: I) {
        for item in iter {
            self.give(item);
        }
    }
    /// Transmits a pre-packed batch of data.
    #[inline]
    pub fn give_content(&mut self, message: &mut Vec<D>) {
        if !message.is_empty() {
            self.buffer.give_vec(message);
        }
    }
}

impl<'a, T: Timestamp, C: Container, P: Push<BundleCore<T, C>>+'a> Drop for AutoflushSessionCore<'a, T, C, P> where T: Eq+Clone+'a, C: 'a {
    fn drop(&mut self) {
        self.buffer.cease();
    }
}
