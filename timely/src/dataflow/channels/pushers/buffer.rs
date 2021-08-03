//! Buffering and session mechanisms to provide the appearance of record-at-a-time sending,
//! with the performance of batched sends.

use crate::{Container, ContainerBuilder, Data};
use crate::dataflow::channels::{Bundle, Message};
use crate::progress::Timestamp;
use crate::dataflow::operators::Capability;
use crate::communication::Push;

/// Buffers data sent at the same time, for efficient communication.
///
/// The `Buffer` type should be used by calling `session` with a time, which checks whether
/// data must be flushed and creates a `Session` object which allows sending at the given time.
pub struct Buffer<T, C: Container, P: Push<Bundle<T, C>>> {
    /// the currently open time, if it is open
    time: Option<T>,
    /// a buffer for records, to send at self.time
    buffer: C::Builder,
    /// The pusher receiving batches of data
    pusher: P,
}

// Cannot derive `Debug` for [Buffer] because we cannot express the constraint that
// `C::Builder: Debug`.
impl<T: ::std::fmt::Debug, C: Container, P: Push<Bundle<T, C>>+::std::fmt::Debug> ::std::fmt::Debug for Buffer<T, C, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("UnorderedHandle");
        debug.field("time", &self.time);
        debug.field("pusher", &self.pusher);
        debug.finish_non_exhaustive()
    }
}

impl<T, C: Container, P: Push<Bundle<T, C>>> Buffer<T, C, P> where T: Eq+Clone {

    /// Creates a new `Buffer`.
    pub fn new(pusher: P) -> Buffer<T, C, P> {
        Buffer {
            time: None,
            buffer: C::Builder::with_capacity(C::default_length()),
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
    pub fn autoflush_session(&mut self, cap: Capability<T>) -> AutoflushSession<T, C, P> where T: Timestamp {
        if let Some(true) = self.time.as_ref().map(|x| x != cap.time()) { self.flush(); }
        self.time = Some(cap.time().clone());
        AutoflushSession {
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
        self.pusher.push(&mut None);
    }

    /// moves the contents of
    fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let time = self.time.as_ref().unwrap().clone();
            let mut buffer = ::std::mem::replace(&mut self.buffer, C::Builder::new()).build();
            Message::push_at(&mut buffer, time, &mut self.pusher);
            self.buffer = C::Builder::with_allocation(buffer);
        }
    }

    // Gives an entire container at a specific time.
    fn give_container(&mut self, vector: &mut C) {
        // flush to ensure fifo-ness
        if !self.buffer.is_empty() {
            self.flush();
        }

        let time = self.time.as_ref().expect("Buffer::give_vec(): time is None.").clone();
        Message::push_at(vector, time, &mut self.pusher);
    }
}

impl<T, C: Container, P: Push<Bundle<T, C>>> Buffer<T, C, P> where T: Eq+Clone {
    // internal method for use by `Session`.
    fn give<D>(&mut self, data: D) where C: Container<Inner=D> {
        self.buffer.push(data);
        // assert!(self.buffer.capacity() == Message::<O::Data>::default_length());
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }
}

impl<T, D: Data, P: Push<Bundle<T, Vec<D>>>> Buffer<T, Vec<D>, P> where T: Eq+Clone {
    // Gives an entire message at a specific time.
    fn give_vec(&mut self, vector: &mut Vec<D>) {
        // flush to ensure fifo-ness
        if !self.buffer.is_empty() {
            self.flush();
        }

        let time = self.time.as_ref().expect("Buffer::give_vec(): time is None.").clone();
        Message::push_at(vector, time, &mut self.pusher);
    }
}


/// An output session for sending records at a specified time.
///
/// The `Session` struct provides the user-facing interface to an operator output, namely
/// the `Buffer` type. A `Session` wraps a session of output at a specified time, and
/// avoids what would otherwise be a constant cost of checking timestamp equality.
pub struct Session<'a, T, C: Container, P: Push<Bundle<T, C>>+'a> where T: Eq+Clone+'a, C: 'a {
    buffer: &'a mut Buffer<T, C, P>,
}

impl<'a, T, D: Data, P: Push<Bundle<T, Vec<D>>>+'a> Session<'a, T, Vec<D>, P>  where T: Eq+Clone+'a, D: 'a {
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

impl<'a, T, C: Container, P: Push<Bundle<T, C>>+'a> Session<'a, T, C, P>  where T: Eq+Clone+'a, C: 'a {

    /// Provides a fully formed container for senders which can use this type.
    #[inline]
    pub fn give_container(&mut self, message: &mut C) {
        if !message.is_empty() {
            self.buffer.give_container(message);
        }
    }
}

/// A session which will flush itself when dropped.
pub struct AutoflushSession<'a, T: Timestamp, D: Container, P: Push<Bundle<T, D>>+'a> where
    T: Eq+Clone+'a, D: 'a {
    /// A reference to the underlying buffer.
    buffer: &'a mut Buffer<T, D, P>,
    /// The capability being used to send the data.
    _capability: Capability<T>,
}

impl<'a, T: Timestamp, C: Container, P: Push<Bundle<T, C>>+'a> AutoflushSession<'a, T, C, P> where T: Eq+Clone+'a, C: 'a {
    /// Transmits a single record.
    #[inline]
    pub fn give<D>(&mut self, data: D) where C: Container<Inner=D> {
        self.buffer.give(data);
    }
    /// Transmits records produced by an iterator.
    #[inline]
    pub fn give_iterator<D, I: Iterator<Item=D>>(&mut self, iter: I) where C: Container<Inner=D> {
        for item in iter {
            self.give(item);
        }
    }
}

impl<'a, T: Timestamp, D: Data, P: Push<Bundle<T, Vec<D>>>+'a> AutoflushSession<'a, T, Vec<D>, P> where T: Eq+Clone+'a, D: 'a {
    /// Transmits a pre-packed batch of data.
    #[inline]
    pub fn give_content(&mut self, message: &mut Vec<D>) {
        if !message.is_empty() {
            self.buffer.give_vec(message);
        }
    }
}

impl<'a, T: Timestamp, D: Container, P: Push<Bundle<T, D>>+'a> Drop for AutoflushSession<'a, T, D, P> where T: Eq+Clone+'a, D: 'a {
    fn drop(&mut self) {
        self.buffer.cease();
    }
}
