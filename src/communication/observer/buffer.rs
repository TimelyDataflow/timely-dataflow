use communication::{Message, Observer};

pub struct Buffer<O: Observer> {
    time: Option<O::Time>,  // the currently open time, if it is open
    buffer: Vec<O::Data>,   // a buffer for records, to send at self.time
    observer: O,
}

impl<O: Observer> Buffer<O> where O::Time: Eq+Clone {

    /// Constructs a new Buffer to wrap a supplied Observer
    pub fn new(obs: O) -> Buffer<O> {
        Buffer {
            time: None,
            buffer: Vec::with_capacity(Message::<O::Data>::default_length()),
            observer: obs,
        }
    }
    /// Flushes any buffered data, calls observer.shut if it was open, and sets self.time = None.
    /// Important to call this to ensure progress; ideally do it for the user, rather than expect
    /// that they will do it.
    pub fn flush_and_shut(&mut self) {
        if self.buffer.len() > 0 {
            assert!(self.time.is_some());
            self.flush();
        }
        if let Some(time) = self.time.take() {
            self.observer.shut(&time);
        }
    }
    /// Returns a Session, which accepts data to send at the associated time
    pub fn session(&mut self, time: &O::Time) -> Session<O> {
        self.set_time(time.clone());
        Session { buffer: self }
    }

    pub fn inner(&mut self) -> &mut O {
        &mut self.observer
    }

    // puts the buffer in a state where self.time == Some(time) and observer.open(&time) called
    // may flush data, call observer.shut, etc., as appropriate.
    fn set_time(&mut self, time: O::Time) {
        // if we have an open time that is different, flush and shut it
        if self.time.as_ref().map(|x| x != &time).unwrap_or(false)  {
            self.flush_and_shut();
        }
        // if time is currently un-set, open it up
        if self.time.is_none() {
            self.observer.open(&time);
            self.time = Some(time);
        }
    }
    /// moves the contents of
    fn flush(&mut self) {
        let mut message = Message::from_typed(&mut self.buffer);
        self.observer.give(&mut message);
        self.buffer = message.into_typed();
        if self.buffer.capacity() != Message::<O::Data>::default_length() {
            // ALLOC : We sent along typed data, so would expect a Vec::new()
            assert!(self.buffer.capacity() == 0);
            self.buffer = Vec::with_capacity(Message::<O::Data>::default_length());
        }
        self.buffer.clear();
    }
    fn give(&mut self, data: O::Data) {
        self.buffer.push(data);
        assert!(self.buffer.capacity() == Message::<O::Data>::default_length());
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }
    /// Gives an entire message at a specific time.
    fn give_message(&mut self, message: &mut Message<O::Data>) {
        // flush to ensure fifo-ness
        if self.buffer.len() > 0 {
            self.flush();
        }
        self.observer.give(message);
    }

}

pub struct Session<'a, O: Observer+'a> where O::Time: Eq+Clone+'a, O::Data: 'a {
    buffer: &'a mut Buffer<O>,
}

impl<'a, O: Observer+'a> Session<'a, O>  where O::Time: Eq+Clone+'a, O::Data: 'a {
    #[inline(always)]
    pub fn give(&mut self, data: O::Data) {
        self.buffer.give(data);
    }
    pub fn give_iterator<I: Iterator<Item=O::Data>>(&mut self, iter: I) {
        for item in iter {
            self.give(item);
        }
    }
    pub fn give_message(&mut self, message: &mut Message<O::Data>) {
        if message.len() > 0 {
            self.buffer.give_message(message);
        }
    }
}
