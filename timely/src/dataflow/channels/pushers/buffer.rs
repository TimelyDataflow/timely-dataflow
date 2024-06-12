//! Buffering and session mechanisms to provide the appearance of record-at-a-time sending,
//! with the performance of batched sends.

use crate::communication::Push;
use crate::container::{ContainerBuilder, CapacityContainerBuilder, PushInto};
use crate::dataflow::channels::{Bundle, Message};
use crate::dataflow::operators::Capability;
use crate::progress::Timestamp;
use crate::Container;

/// Buffers data sent at the same time, for efficient communication.
///
/// The `Buffer` type should be used by calling `session` with a time, which checks whether
/// data must be flushed and creates a `Session` object which allows sending at the given time.
#[derive(Debug)]
pub struct Buffer<T, CB, P> {
    /// The currently open time, if it is open.
    time: Option<T>,
    /// A builder for containers, to send at `self.time`.
    builder: CB,
    /// The pusher to send data downstream.
    pusher: P,
}

impl<T, CB: Default, P> Buffer<T, CB, P> {
    /// Creates a new `Buffer`.
    pub fn new(pusher: P) -> Self {
        Self {
            time: None,
            builder: Default::default(),
            pusher,
        }
    }

    /// Returns a reference to the inner `P: Push` type.
    ///
    /// This is currently used internally, and should not be used without some care.
    pub fn inner(&mut self) -> &mut P { &mut self.pusher }

    /// Access the builder. Immutable access to prevent races with flushing
    /// the underlying buffer.
    pub fn builder(&self) -> &CB {
        &self.builder
    }
}

impl<T, C: Container, P: Push<Bundle<T, C>>> Buffer<T, CapacityContainerBuilder<C>, P> where T: Eq+Clone {
    /// Returns a `Session`, which accepts data to send at the associated time
    #[inline]
    pub fn session(&mut self, time: &T) -> Session<T, CapacityContainerBuilder<C>, P> {
        self.session_with_builder(time)
    }

    /// Allocates a new `AutoflushSession` which flushes itself on drop.
    #[inline]
    pub fn autoflush_session(&mut self, cap: Capability<T>) -> AutoflushSession<T, CapacityContainerBuilder<C>, P> where T: Timestamp {
        self.autoflush_session_with_builder(cap)
    }

    /// Gives an entire container at the current time.
    fn give_container(&mut self, container: &mut C) {
        if !container.is_empty() {
            self.builder.push_container(container);
            self.extract_and_send();
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

impl<T, CB: ContainerBuilder, P: Push<Bundle<T, CB::Container>>> Buffer<T, CB, P> where T: Eq+Clone {
    /// Flushes all data and pushes a `None` to `self.pusher`, indicating a flush.
    pub fn cease(&mut self) {
        self.flush();
        self.pusher.push(&mut None);
    }

    /// Extract pending data from the builder, but not forcing a flush.
    #[inline]
    fn extract_and_send(&mut self) {
        while let Some(container) = self.builder.extract() {
            let time = self.time.as_ref().unwrap().clone();
            Message::push_at(container, time, &mut self.pusher);
        }
    }

    /// Flush the builder, forcing all its contents to be written.
    #[inline]
    fn flush(&mut self) {
        while let Some(container) = self.builder.finish() {
            let time = self.time.as_ref().unwrap().clone();
            Message::push_at(container, time, &mut self.pusher);
        }
    }
}

impl<T, CB, P, D> PushInto<D> for Buffer<T, CB, P>
where
    T: Eq+Clone,
    CB: ContainerBuilder + PushInto<D>,
    P: Push<Bundle<T, CB::Container>>
{
    #[inline]
    fn push_into(&mut self, item: D) {
        self.builder.push_into(item);
        self.extract_and_send();
    }
}

/// An output session for sending records at a specified time.
///
/// The `Session` struct provides the user-facing interface to an operator output, namely
/// the `Buffer` type. A `Session` wraps a session of output at a specified time, and
/// avoids what would otherwise be a constant cost of checking timestamp equality.
pub struct Session<'a, T, CB, P> {
    buffer: &'a mut Buffer<T, CB, P>,
}

impl<'a, T, C: Container, P> Session<'a, T, CapacityContainerBuilder<C>, P>
where
    T: Eq + Clone + 'a,
    P: Push<Bundle<T, C>> + 'a,
{
    /// Provide a container at the time specified by the [Session].
    pub fn give_container(&mut self, container: &mut C) {
        self.buffer.give_container(container)
    }
}

impl<'a, T, CB, P> Session<'a, T, CB, P>
where
    T: Eq + Clone + 'a,
    CB: ContainerBuilder + 'a,
    P: Push<Bundle<T, CB::Container>> + 'a
{
    /// Access the builder. Immutable access to prevent races with flushing
    /// the underlying buffer.
    pub fn builder(&self) -> &CB {
        self.buffer.builder()
    }

    /// Provides one record at the time specified by the `Session`.
    #[inline]
    pub fn give<D>(&mut self, data: D) where CB: PushInto<D> {
        self.push_into(data);
    }

    /// Provides an iterator of records at the time specified by the `Session`.
    #[inline]
    pub fn give_iterator<I>(&mut self, iter: I)
    where
        I: Iterator,
        CB: PushInto<I::Item>,
    {
        for item in iter {
            self.push_into(item);
        }
    }
}

impl<'a, T, CB, P, D> PushInto<D> for Session<'a, T, CB, P>
where
    T: Eq + Clone + 'a,
    CB: ContainerBuilder + PushInto<D> + 'a,
    P: Push<Bundle<T, CB::Container>> + 'a,
{
    #[inline]
    fn push_into(&mut self, item: D) {
        self.buffer.push_into(item);
    }
}

/// A session which will flush itself when dropped.
pub struct AutoflushSession<'a, T, CB, P>
where
    T: Timestamp + 'a,
    CB: ContainerBuilder + 'a,
    P: Push<Bundle<T, CB::Container>> + 'a,
{
    /// A reference to the underlying buffer.
    buffer: &'a mut Buffer<T, CB, P>,
    /// The capability being used to send the data.
    _capability: Capability<T>,
}

impl<'a, T, CB, P> AutoflushSession<'a, T, CB, P>
where
    T: Timestamp + 'a,
    CB: ContainerBuilder + 'a,
    P: Push<Bundle<T, CB::Container>> + 'a,
{
    /// Transmits a single record.
    #[inline]
    pub fn give<D>(&mut self, data: D)
    where
        CB: PushInto<D>,
    {
        self.push_into(data);
    }

    /// Transmits records produced by an iterator.
    #[inline]
    pub fn give_iterator<I, D>(&mut self, iter: I)
    where
        I: Iterator<Item=D>,
        CB: PushInto<D>,
    {
        for item in iter {
            self.push_into(item);
        }
    }
}
impl<'a, T, CB, P, D> PushInto<D> for AutoflushSession<'a, T, CB, P>
where
    T: Timestamp + 'a,
    CB: ContainerBuilder + PushInto<D> + 'a,
    P: Push<Bundle<T, CB::Container>> + 'a,
{
    #[inline]
    fn push_into(&mut self, item: D) {
        self.buffer.push_into(item);
    }
}

impl<'a, T, CB, P> Drop for AutoflushSession<'a, T, CB, P>
where
    T: Timestamp + 'a,
    CB: ContainerBuilder + 'a,
    P: Push<Bundle<T, CB::Container>> + 'a,
{
    fn drop(&mut self) {
        self.buffer.cease();
    }
}
