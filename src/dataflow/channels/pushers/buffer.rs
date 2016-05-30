use dataflow::channels::Content;
use progress::Timestamp;
use dataflow::operators::Capability;
use timely_communication::Push;

/// Buffers data sent at the same time, for efficient communication.
///
/// The `Buffer` type should be used by calling `session` with a time, which checks whether
/// data must be flushed and creates a `Session` object which allows sending at the given time. 
pub struct Buffer<T, D, P: Push<(T, Content<D>)>> {
    time: Option<T>,  // the currently open time, if it is open
    buffer: Vec<D>,   // a buffer for records, to send at self.time
    pusher: P,
}

impl<T, D, P: Push<(T, Content<D>)>> Buffer<T, D, P> where T: Eq+Clone {

    /// Creates a new `Buffer`.
    pub fn new(pusher: P) -> Buffer<T, D, P> {
        Buffer {
            time: None,
            buffer: Vec::with_capacity(Content::<D>::default_length()),
            pusher: pusher,
        }
    }

    /// Returns a `Session`, which accepts data to send at the associated time
    pub fn session(&mut self, time: &T) -> Session<T, D, P> {
        if let Some(true) = self.time.as_ref().map(|x| x != time) { self.flush(); }
        self.time = Some(time.clone());
        Session { buffer: self }
    }

    pub fn autoflush_session(&mut self, cap: Capability<T>) -> AutoflushSession<T, D, P> where T: Timestamp {
        if let Some(true) = self.time.as_ref().map(|x| *x != cap.time()) { self.flush(); }
        self.time = Some(cap.time());
        AutoflushSession {
            buffer: self,
            capability: cap,
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
            Content::push_at(&mut self.buffer, time, &mut self.pusher);
        }
    }

    // internal method for use by `Session`.
    fn give(&mut self, data: D) {
        self.buffer.push(data);
        // assert!(self.buffer.capacity() == Message::<O::Data>::default_length());
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }

    // Gives an entire message at a specific time.
    fn give_content(&mut self, content: &mut Content<D>) {
        // flush to ensure fifo-ness
        if !self.buffer.is_empty() {
            self.flush();
        }

        let time = self.time.as_ref().unwrap().clone();
        let data = ::std::mem::replace(content, Content::Typed(Vec::new()));
        let mut message = Some((time, data));

        self.pusher.push(&mut message);
        if let Some((_, data)) = message {
            *content = data;
        }
    }
}


/// An output session for sending records at a specified time.
///
/// The `Session` struct provides the user-facing interface to an operator output, namely
/// the `Buffer` type. A `Session` wraps a session of output at a specified time, and 
/// avoids what would otherwise be a constant cost of checking timestamp equality.
pub struct Session<'a, T, D, P: Push<(T, Content<D>)>+'a> where T: Eq+Clone+'a, D: 'a {
    buffer: &'a mut Buffer<T, D, P>,
}

impl<'a, T, D, P: Push<(T, Content<D>)>+'a> Session<'a, T, D, P>  where T: Eq+Clone+'a, D: 'a {
    /// Provides one record at the time specified by the `Session`.
    #[inline(always)]
    pub fn give(&mut self, data: D) {
        self.buffer.give(data);
    }
    /// Provides an iterator of records at the time specified by the `Session`.
    #[inline(always)]
    pub fn give_iterator<I: Iterator<Item=D>>(&mut self, iter: I) {
        for item in iter {
            self.give(item);
        }
    }
    /// Provides a fully formed `Content<D>` message for senders which can use this type.
    ///
    /// The `Content` type is the backing memory for communication in timely, and it can
    /// often be more efficient to re-use this memory rather than have timely allocate 
    /// new backing memory.
    #[inline(always)]
    pub fn give_content(&mut self, message: &mut Content<D>) {
        if message.len() > 0 {
            self.buffer.give_content(message);
        }
    }
}

pub struct AutoflushSession<'a, T: Timestamp, D, P: Push<(T, Content<D>)>+'a> where
    T: Eq+Clone+'a, D: 'a {

    pub buffer: &'a mut Buffer<T, D, P>,
    pub capability: Capability<T>,
}

impl<'a, T: Timestamp, D, P: Push<(T, Content<D>)>+'a> AutoflushSession<'a, T, D, P> where T: Eq+Clone+'a, D: 'a {
    #[inline(always)]
    pub fn give(&mut self, data: D) {
        self.buffer.give(data);
    }

    #[inline(always)]
    pub fn give_iterator<I: Iterator<Item=D>>(&mut self, iter: I) {
        for item in iter {
            self.give(item);
        }
    }

    #[inline(always)]
    pub fn give_content(&mut self, message: &mut Content<D>) {
        if message.len() > 0 {
            self.buffer.give_content(message);
        }
    }
}

impl<'a, T: Timestamp, D, P: Push<(T, Content<D>)>+'a> Drop for AutoflushSession<'a, T, D, P> where T: Eq+Clone+'a, D: 'a {
    fn drop(&mut self) {
        self.buffer.cease();
    }
}
