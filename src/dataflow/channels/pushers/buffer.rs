use dataflow::channels::Content;
use timely_communication::Push;

pub struct Buffer<T, D, P: Push<(T, Content<D>)>> {
    time: Option<T>,  // the currently open time, if it is open
    buffer: Vec<D>,   // a buffer for records, to send at self.time
    pusher: P,
}

impl<T, D, P: Push<(T, Content<D>)>> Buffer<T, D, P> where T: Eq+Clone {

    pub fn new(pusher: P) -> Buffer<T, D, P> {
        Buffer {
            time: None,
            buffer: Vec::with_capacity(Content::<D>::default_length()),
            pusher: pusher,
        }
    }

    /// Returns a Session, which accepts data to send at the associated time
    pub fn session(&mut self, time: &T) -> Session<T, D, P> {
        if let Some(true) = self.time.as_ref().map(|x| x != time) { self.flush(); }
        self.time = Some(time.clone());
        Session { buffer: self }
    }

    pub fn inner(&mut self) -> &mut P { &mut self.pusher }

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

    fn give(&mut self, data: D) {
        self.buffer.push(data);
        // assert!(self.buffer.capacity() == Message::<O::Data>::default_length());
        if self.buffer.len() == self.buffer.capacity() {
            self.flush();
        }
    }

    /// Gives an entire message at a specific time.
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



pub struct Session<'a, T, D, P: Push<(T, Content<D>)>+'a> where T: Eq+Clone+'a, D: 'a {
    buffer: &'a mut Buffer<T, D, P>,
}

impl<'a, T, D, P: Push<(T, Content<D>)>+'a> Session<'a, T, D, P>  where T: Eq+Clone+'a, D: 'a {
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
