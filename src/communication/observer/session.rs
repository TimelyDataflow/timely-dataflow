use communication::{Message, Observer};

// Attempt at RAII for observers. Intended to prevent mis-sequencing of open/push/shut.
pub struct Session<'a, O:Observer+'a> {
    time: O::Time,
    buffer: Vec<O::Data>,
    observer: &'a mut O,
}

impl<'a, O:Observer> Drop for Session<'a, O> {
    #[inline] fn drop(&mut self) {
        if self.buffer.len() > 0 {
            let mut message = Message::from_typed(&mut self.buffer);
            // let mut message = Message::Typed(::std::mem::replace(&mut self.buffer, Vec::new()));
            self.observer.give(&mut message);
            self.buffer = message.into_typed(4096);
            // self.buffer = if let Message::Typed(mut buffer) = message { buffer.clear(); buffer }
            //               else { Vec::with_capacity(4096) };
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
            // let mut message = Message::Typed(::std::mem::replace(&mut self.buffer, Vec::new()));
            self.observer.give(&mut message);
            self.buffer = message.into_typed(4096);
            // self.buffer = if let Message::Typed(mut buffer) = message { buffer.clear(); buffer }
            //               else { Vec::with_capacity(4096) };
        }
    }
}
