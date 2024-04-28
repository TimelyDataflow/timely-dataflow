//! Buffering and session mechanisms to provide the appearance of record-at-a-time sending,
//! with the performance of batched sends.

use crate::communication::Push;
use crate::container::{ContainerBuilder, CapacityContainerBuilder, PushContainer, PushInto};
use crate::dataflow::channels::{Bundle, Message};
use crate::dataflow::operators::Capability;
use crate::progress::Timestamp;
use crate::{Container, Data};

/// Buffers data sent at the same time, for efficient communication.
///
/// The `Buffer` type should be used by calling `session` with a time, which checks whether
/// data must be flushed and creates a `Session` object which allows sending at the given time.
#[derive(Debug)]
pub struct Buffer<T, B, P> {
    /// the currently open time, if it is open
    time: Option<T>,
    /// a buffer for records, to send at self.time
    buffer: B,
    pusher: P,
}

impl<T, B: Default, P> Buffer<T, B, P> {
    /// Creates a new `Buffer`.
    pub fn new(pusher: P) -> Self {
        Self {
            time: None,
            buffer: Default::default(),
            pusher,
        }
    }

    /// Returns a reference to the inner `P: Push` type.
    ///
    /// This is currently used internally, and should not be used without some care.
    pub fn inner(&mut self) -> &mut P { &mut self.pusher }
}

impl<T, C: Container, P: Push<Bundle<T, C>>> Buffer<T, CapacityContainerBuilder<C>, P> where T: Eq+Clone {
    /// Returns a `Session`, which accepts data to send at the associated time
    pub fn session(&mut self, time: &T) -> Session<T, CapacityContainerBuilder<C>, P> {
        if let Some(true) = self.time.as_ref().map(|x| x != time) { self.flush(); }
        self.time = Some(time.clone());
        Session { buffer: self }
    }
    /// Allocates a new `AutoflushSession` which flushes itself on drop.
    pub fn autoflush_session(&mut self, cap: Capability<T>) -> AutoflushSession<T, CapacityContainerBuilder<C>, P> where T: Timestamp {
        if let Some(true) = self.time.as_ref().map(|x| x != cap.time()) { self.flush(); }
        self.time = Some(cap.time().clone());
        AutoflushSession {
            buffer: self,
            _capability: cap,
        }
    }
}

impl<T, CB: ContainerBuilder, P: Push<Bundle<T, CB::Container>>> Buffer<T, CB, P> where T: Eq+Clone {
    /// Returns a `Session`, which accepts data to send at the associated time
    pub fn session_with_builder(&mut self, time: &T) -> Session<T, CB, P> {
        if let Some(true) = self.time.as_ref().map(|x| x != time) { self.flush(); }
        self.time = Some(time.clone());
        Session { buffer: self }
    }
    /// Allocates a new `AutoflushSession` which flushes itself on drop.
    pub fn autoflush_session_with_builder(&mut self, cap: Capability<T>) -> AutoflushSession<T, CB, P> where T: Timestamp {
        if let Some(true) = self.time.as_ref().map(|x| x != cap.time()) { self.flush(); }
        self.time = Some(cap.time().clone());
        AutoflushSession {
            buffer: self,
            _capability: cap,
        }
    }
}

impl<T, B: ContainerBuilder, P: Push<Bundle<T, B::Container>>> Buffer<T, B, P> where T: Eq+Clone {
    /// Flushes all data and pushes a `None` to `self.pusher`, indicating a flush.
    pub fn cease(&mut self) {
        self.flush();
        self.pusher.push(&mut None);
    }

    /// moves the contents of
    #[inline]
    fn flush(&mut self) {
        for mut container in self.buffer.finish() {
            let time = self.time.as_ref().unwrap().clone();
            Message::push_at(&mut container, time, &mut self.pusher);
        }
    }

    // Gives an entire container at a specific time.
    fn give_container(&mut self, container: &mut B::Container) {
        if !container.is_empty() {
            // flush to ensure fifo-ness
            self.flush();

            let time = self.time.as_ref().expect("Buffer::give_container(): time is None.").clone();
            Message::push_at(container, time, &mut self.pusher);
        }
    }
}

impl<T, B: ContainerBuilder, P: Push<Bundle<T, B::Container>>> Buffer<T, B, P>
where
    T: Eq+Clone,
    B::Container: PushContainer,
{
    // internal method for use by `Session`.
    #[inline]
    fn give<D: PushInto<B::Container>>(&mut self, data: D) {
        self.buffer.push(data);

        // Extract finished data, but leave unfinished data behind. This is different
        // from calling `flush`!
        for mut container in self.buffer.extract() {
            let time = self.time.as_ref().unwrap().clone();
            Message::push_at(&mut container, time, &mut self.pusher);
        }
    }
}

impl<T, B, D: Data, P: Push<Bundle<T, Vec<D>>>> Buffer<T, B, P>
where
    T: Eq+Clone,
    B: ContainerBuilder<Container=Vec<D>>,
{
    // Gives an entire message at a specific time.
    fn give_vec(&mut self, vector: &mut Vec<D>) {
        // flush to ensure fifo-ness
        self.flush();

        let time = self.time.as_ref().expect("Buffer::give_vec(): time is None.").clone();
        Message::push_at(vector, time, &mut self.pusher);
    }
}


/// An output session for sending records at a specified time.
///
/// The `Session` struct provides the user-facing interface to an operator output, namely
/// the `Buffer` type. A `Session` wraps a session of output at a specified time, and
/// avoids what would otherwise be a constant cost of checking timestamp equality.
pub struct Session<'a, T, B, P>
where
    T: Eq + Clone + 'a,
    B: ContainerBuilder + 'a,
    P: Push<Bundle<T, B::Container>> + 'a,
{
    buffer: &'a mut Buffer<T, B, P>,
}

impl<'a, T, B, P> Session<'a, T, B, P>
where
    T: Eq + Clone + 'a,
    B: ContainerBuilder + 'a,
    P: Push<Bundle<T, B::Container>> + 'a
{
    /// Provide a container at the time specified by the [Session].
    pub fn give_container(&mut self, container: &mut B::Container) {
        self.buffer.give_container(container)
    }
}

impl<'a, T, B, P: Push<Bundle<T, B::Container>>+'a> Session<'a, T, B, P>
where
    T: Eq + Clone + 'a,
    B: ContainerBuilder + 'a,
    B::Container: PushContainer,
{
    /// Provides one record at the time specified by the `Session`.
    #[inline]
    pub fn give<D: PushInto<B::Container>>(&mut self, data: D) {
        self.buffer.give(data);
    }
    /// Provides an iterator of records at the time specified by the `Session`.
    #[inline]
    pub fn give_iterator<I, D>(&mut self, iter: I)
    where
        I: Iterator<Item=D>,
        D: PushInto<B::Container>,
    {
        for item in iter {
            self.give(item);
        }
    }
}

impl<'a, T, B, D, P> Session<'a, T, B, P>
where
    T: Eq + Clone + 'a,
    B: ContainerBuilder<Container=Vec<D>>,
    D: Data,
    P: Push<Bundle<T, Vec<D>>> + 'a,
{
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
pub struct AutoflushSession<'a, T, B, P>
where
    T: Timestamp + 'a,
    B: ContainerBuilder + 'a,
    P: Push<Bundle<T, B::Container>> + 'a,
{
    /// A reference to the underlying buffer.
    buffer: &'a mut Buffer<T, B, P>,
    /// The capability being used to send the data.
    _capability: Capability<T>,
}

impl<'a, T: Timestamp, B: ContainerBuilder, P: Push<Bundle<T, B::Container>>+'a> AutoflushSession<'a, T, B, P> where T: Eq+Clone+'a, B: 'a {
    /// Transmits a single record.
    #[inline]
    pub fn give<D: PushInto<B::Container>>(&mut self, data: D) where B::Container: PushContainer {
        self.buffer.give(data);
    }
    /// Transmits records produced by an iterator.
    #[inline]
    pub fn give_iterator<I, D>(&mut self, iter: I)
        where
            I: Iterator<Item=D>,
            D: PushInto<B::Container>,
            B::Container: PushContainer,
    {
        for item in iter {
            self.give(item);
        }
    }
}

impl<'a, T: Timestamp, B: ContainerBuilder, P: Push<Bundle<T, B::Container>>+'a> Drop for AutoflushSession<'a, T, B, P> where T: Eq+Clone+'a, B: 'a {
    fn drop(&mut self) {
        self.buffer.cease();
    }
}
