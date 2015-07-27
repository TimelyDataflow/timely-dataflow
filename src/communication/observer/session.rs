use communication::{Message, Observer};

// RAII for Observer sessions. Intended to prevent mis-sequencing of open/give/shut.
//
pub struct Session<'a, O:Observer+'a> {
    time: O::Time,
    buffer: Vec<O::Data>,
    observer: &'a mut O,
}

impl<'a, O:Observer> Drop for Session<'a, O> {
    #[inline] fn drop(&mut self) {
        if self.buffer.len() > 0 {
            let mut message = Message::from_typed(&mut self.buffer);
            self.observer.give(&mut message);
            self.buffer = message.into_typed(4096);
        }
        self.observer.shut(&self.time);
    }
}

impl<'a, O:Observer> Session<'a, O> where O::Time: Clone {
    pub fn new(obs: &'a mut O, time: &O::Time) -> Session<'a, O> {
        Session {
            time: (*time).clone(),
            buffer: Vec::with_capacity(4096),
            observer: obs,
        }
    }
    #[inline(always)] pub fn give(&mut self, data: O::Data) {
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            let mut message = Message::from_typed(&mut self.buffer);
            self.observer.give(&mut message);
            self.buffer = message.into_typed(4096);
        }
    }
    pub fn new_time(&mut self, time: &O::Time) {
        if self.buffer.len() > 0 {
            let mut message = Message::from_typed(&mut self.buffer);
            self.observer.give(&mut message);
            self.buffer = message.into_typed(4096);
            self.buffer.clear();
        }
        self.observer.shut(&self.time);
        self.time = (*time).clone();
        self.observer.open(&self.time);
    }
}
