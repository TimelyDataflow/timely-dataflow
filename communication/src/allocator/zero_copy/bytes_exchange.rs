use bytes::arc::Bytes;
use super::bytes_slab::BytesSlab;

/// A target for `Bytes`.
pub trait BytesPush { fn push(&mut self, bytes: Bytes); }
/// A source for `Bytes`.
pub trait BytesPull { fn pull(&mut self) -> Option<Bytes>; }

// std::sync::mpsc implementations.
use ::std::sync::mpsc::{Sender, Receiver};
impl BytesPush for Sender<Bytes> {
    fn push(&mut self, bytes: Bytes) {
        self.send(bytes)
            .expect("unable to send Bytes");
    }
}

impl BytesPull for Receiver<Bytes> {
    fn pull(&mut self) -> Option<Bytes> {
        self.try_recv()
            .ok()
    }
}

// Arc<Mutex<VecDeque<Bytes>>> implementations.
use ::std::sync::{Arc, Mutex};
use ::std::collections::VecDeque;
impl BytesPush for Arc<Mutex<VecDeque<Bytes>>> {
    fn push(&mut self, bytes: Bytes) {
        self.lock()
            .expect("unable to lock mutex")
            .push_back(bytes);
    }
}

impl BytesPull for Arc<Mutex<VecDeque<Bytes>>> {
    fn pull(&mut self) -> Option<Bytes> {
        self.lock()
            .expect("unable to lock mutex")
            .pop_front()
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
            self.send.push(self.buffer.extract(valid_len));
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: P) -> Self {
        SendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20),
        }
    }

    pub fn make_valid(&mut self, bytes: usize) {
        self.buffer.make_valid(bytes);
        self.send_buffer();
    }
    pub fn reserve(&mut self, capacity: usize) -> &mut [u8] {

        if self.buffer.empty().len() < capacity {
            self.send_buffer();
            self.buffer.ensure_capacity(capacity);
        }

        assert!(self.buffer.empty().len() >= capacity);
        self.buffer.empty()
    }

    pub fn publish(&mut self) {
        self.send_buffer();
    }
}

impl<P: BytesPush> Drop for SendEndpoint<P> {
    fn drop(&mut self) {
        self.send_buffer();
    }
}