use std::ops::DerefMut;
use bytes::arc::Bytes;

use super::shared_queue::{SharedQueueSend, SharedQueueRecv};
use super::bytes_slab::BytesSlab;

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
    send: SharedQueueSend<Bytes>,
    buffer: BytesSlab,
    // in_progress: Vec<Option<Bytes>>,
    // buffer: Vec<u8>,
    // stash: Vec<Vec<u8>>,
    // default_size: usize,
}

impl BytesSendEndpoint {
    // /// Attempts to recover in-use buffers once uniquely owned.
    // fn harvest_shared(&mut self) {
    //     for shared in self.in_progress.iter_mut() {
    //         if let Some(bytes) = shared.take() {
    //             match bytes.try_recover::<Vec<u8>>() {
    //                 Ok(mut vec)    => { vec.clear(); self.stash.push(vec); },
    //                 Err(bytes) => { *shared = Some(bytes); },
    //             }
    //         }
    //     }
    //     self.in_progress.retain(|x| x.is_some());
    // }

    /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
    fn send_buffer(&mut self) {

        let valid_len = self.buffer.valid().len();
        if valid_len > 0 {

            let to_send = self.buffer.extract(valid_len);

            // let buffer = ::std::mem::replace(&mut self.buffer, Vec::new());
            // let buffer_len = buffer.len();
            // let mut bytes = Bytes::from(buffer);
            // let to_send = bytes.extract_to(buffer_len);

            self.send.push(to_send);
            // self.in_progress.push(Some(bytes));
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: SharedQueueSend<Bytes>) -> Self {
        BytesSendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20),
            // in_progress: Vec::new(),
            // buffer: Vec::new(),
            // stash: Vec::new(),
            // default_size: 1 << 20,
        }
    }
}

impl BytesSendEndpoint {

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

        // // println!("reserving {:?} bytes", capacity);
        // // if self.send.is_empty() {
        // //     self.send_buffer();
        // // }

        // // If we don't have enough capacity in `self.buffer`...
        // if self.buffer.capacity() < capacity + self.buffer.len() {
        //     self.send_buffer();
        //     if capacity > self.default_size {
        //         self.buffer = Vec::with_capacity(capacity);
        //     }
        //     else {
        //         if self.stash.is_empty() {
        //             // Attempt to recover shared buffers.
        //             self.harvest_shared();
        //         }
        //         self.buffer = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(self.default_size))
        //     }
        // }

        // self.buffer.make_valid(capacity);
        self.buffer.empty()
    }

    pub fn publish(&mut self) {
        // self.harvest_shared();
        // if self.send.is_empty() {
            self.send_buffer();
        // }
        // else {
        //     // println!("delaying publication!");
        // }
    }
}

impl Drop for BytesSendEndpoint {
    fn drop(&mut self) {
        self.send_buffer();
        // assert!(self.buffer.is_empty());
    }
}

pub struct BytesRecvEndpoint {
    recv: SharedQueueRecv<Bytes>,
}


impl BytesRecvEndpoint {
    pub fn new(queue: SharedQueueRecv<Bytes>) -> Self {
        BytesRecvEndpoint { recv: queue }
    }
}

impl RecvEndpoint for BytesRecvEndpoint {
    type RecvBuffer = Bytes;
    fn receive(&mut self) -> Option<Bytes> {
        self.recv.pop()
    }
}