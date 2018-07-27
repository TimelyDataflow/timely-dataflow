use std::ops::DerefMut;
use bytes::arc::Bytes;

use super::SharedQueue;

/// A type that can allocate send and receive endpoints for byte exchanges.
///
/// The `BytesExchange` intent is that one can abstractly define mechanisms for exchanging
/// bytes between various entities. In some cases this may be between worker threads within
/// a process, in other cases it may be between worker threads and remote processes. At the
/// moment the cardinalities of remote endpoints requires some context and interpretation.
pub trait BytesExchange {
    /// The type of the send endpoint.
    type Send: SendEndpoint+'static;
    /// The type of the receive endpoint.
    type Recv: RecvEndpoint+'static;
    /// Allocates endpoint pairs for a specified worker.
    ///
    /// Importantly, the Send side may share state to coalesce the buffering and
    /// transmission of records. That is why there are `Rc<RefCell<_>>` things there.
    fn next(&mut self) -> Option<(Vec<Self::Send>, Vec<Self::Recv>)>;
}

/// A type that can provide and publish writeable binary buffers.
pub trait SendEndpoint {
    /// The type of the writeable binary buffer.
    type SendBuffer: ::std::io::Write;
    /// Provides a writeable buffer of the requested capacity.
    fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer;
    /// Indicates that it is now appropriate to publish the buffer.
    fn publish(&mut self);
}

/// A type that can provide readable binary buffers.
pub trait RecvEndpoint {
    type RecvBuffer: DerefMut<Target=[u8]>;
    /// Provides a readable buffer.
    fn receive(&mut self) -> Option<Self::RecvBuffer>;
}

pub struct BytesSendEndpoint {
    send: SharedQueue<Bytes>,
    in_progress: Vec<Option<Bytes>>,
    buffer: Vec<u8>,
    stash: Vec<Vec<u8>>,
    default_size: usize,
}

impl BytesSendEndpoint {
    /// Attempts to recover in-use buffers once uniquely owned.
    fn harvest_shared(&mut self) {
        for shared in self.in_progress.iter_mut() {
            if let Some(bytes) = shared.take() {
                match bytes.try_recover::<Vec<u8>>() {
                    Ok(vec)    => { self.stash.push(vec); },
                    Err(bytes) => { *shared = Some(bytes); },
                }
            }
        }
        self.in_progress.retain(|x| x.is_some());
    }

    /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
    fn send_buffer(&mut self) {

        let buffer = ::std::mem::replace(&mut self.buffer, Vec::new());
        let buffer_len = buffer.len();
        if buffer_len > 0 {

            let mut bytes = Bytes::from(buffer);
            let to_send = bytes.extract_to(buffer_len);

            self.send.push(to_send);
            self.in_progress.push(Some(bytes));
        }
        else {
            if buffer.capacity() == self.default_size {
                self.stash.push(buffer);
            }
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: SharedQueue<Bytes>) -> Self {
        BytesSendEndpoint {
            send: queue,
            in_progress: Vec::new(),
            buffer: Vec::new(),
            stash: Vec::new(),
            default_size: 1 << 20,
        }
    }
}

impl SendEndpoint for BytesSendEndpoint {

    type SendBuffer = Vec<u8>;

    fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer {

        // If we don't have enough capacity in `self.buffer`...
        if self.buffer.capacity() < capacity + self.buffer.len() {
            self.send_buffer();
            if capacity > self.default_size {
                self.buffer = Vec::with_capacity(capacity);
            }
            else {
                if self.stash.is_empty() {
                    // Attempt to recover shared buffers.
                    self.harvest_shared();
                }
                self.buffer = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(self.default_size))
            }
        }

        &mut self.buffer
    }

    fn publish(&mut self) {
        self.harvest_shared();
        if self.send.is_empty() {
            self.send_buffer();
        }
    }
}

pub struct BytesRecvEndpoint {
    recv: SharedQueue<Bytes>,
}


impl BytesRecvEndpoint {
    pub fn new(queue: SharedQueue<Bytes>) -> Self {
        BytesRecvEndpoint { recv: queue }
    }
}

impl RecvEndpoint for BytesRecvEndpoint {
    type RecvBuffer = Bytes;
    fn receive(&mut self) -> Option<Bytes> {
        self.recv.pop()
    }
}