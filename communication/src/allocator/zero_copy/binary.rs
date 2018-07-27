use std::io::{Read, Write};

use bytes::arc::Bytes;

use networking::MessageHeader;
use super::{BytesExchange, SharedQueue};
use super::{BytesSendEndpoint, BytesRecvEndpoint};

/// Allocates pairs of byte exchanges for remote workers.
pub struct TcpBytesExchange {
    /// Forward[i,j]: from worker i to process j.
    forward: Vec<Vec<SharedQueue<Bytes>>>,
    /// Reverse[i,j]: to process i from worker j.
    reverse: Vec<Vec<SharedQueue<Bytes>>>,
}

impl BytesExchange for TcpBytesExchange {

    type Send = BytesSendEndpoint;
    type Recv = BytesRecvEndpoint;

    // Returns two vectors of length #processes - 1.
    // The first contains destinations to send to remote processes,
    // The second contains sources to receive from remote processes.
    fn next(&mut self) -> Option<(Vec<Self::Send>, Vec<Self::Recv>)> {

        if !self.forward.is_empty() && !self.reverse.is_empty() {
            Some((
                self.forward.remove(0).into_iter().map(|x| BytesSendEndpoint::new(x)).collect(),
                self.reverse.remove(0).into_iter().map(|x| BytesRecvEndpoint::new(x)).collect(),
            ))
        }
        else {
            None
        }
    }
}

impl TcpBytesExchange {
    pub fn new(forward: Vec<Vec<SharedQueue<Bytes>>>, reverse: Vec<Vec<SharedQueue<Bytes>>>) -> Self {
        TcpBytesExchange {
            forward,
            reverse,
        }
    }
}

// Allocates local and remote queue pairs, respectively.
pub fn allocate_queue_pairs(local: usize, remote: usize) -> (Vec<Vec<SharedQueue<Bytes>>>, Vec<Vec<SharedQueue<Bytes>>>) {

    // type annotations necessary despite return signature because ... Rust.
    let local_to_remote: Vec<Vec<_>> = (0 .. local).map(|_| (0 .. remote).map(|_| SharedQueue::new()).collect()).collect();
    let remote_to_local: Vec<Vec<_>> = (0 .. remote).map(|r| (0 .. local).map(|l| local_to_remote[l][r].clone()).collect()).collect();

    (local_to_remote, remote_to_local)
}

/// Receives serialized data from a `Read`, for example the network.
///
/// The `BinaryReceiver` repeatedly reads binary data from its reader into
/// a binary Bytes slice which can be broken off and handed to recipients as
/// messages become complete.
pub struct BinaryReceiver<R: Read> {

    worker_offset:  usize,

    reader:         R,                          // the generic reader.
    buffer:         Bytes,                      // current working buffer.
    length:         usize,                      // consumed buffer elements.
    targets:        Vec<SharedQueue<Bytes>>,    // to process-local workers.
    log_sender:     ::logging::CommsLogger,     // logging stuffs.

    in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
    stash:          Vec<Vec<u8>>,               // reclaimed and resuable buffers.
    size:           usize,                      // current buffer allocation size.
}

impl<R: Read> BinaryReceiver<R> {

    pub fn new(
        reader: R,
        targets: Vec<SharedQueue<Bytes>>,
        worker_offset: usize,
        log_sender: ::logging::CommsLogger) -> BinaryReceiver<R> {
        BinaryReceiver {
            reader,
            targets,
            log_sender,
            buffer: Bytes::from(vec![0u8; 1 << 20]),
            length: 0,
            in_progress: Vec::new(),
            stash: Vec::new(),
            size: 1 << 20,
            worker_offset,
        }
    }

    // Retire `self.buffer` and acquire a new buffer of at least `self.size` bytes.
    fn refresh_buffer(&mut self) {

        if self.stash.is_empty() {
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

        let self_size = self.size;
        self.stash.retain(|x| x.capacity() == self_size);


        let new_buffer = self.stash.pop().unwrap_or_else(|| vec![0; 1 << self.size]);
        let new_buffer = Bytes::from(new_buffer);
        let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

        self.buffer[.. self.length].copy_from_slice(&old_buffer[.. self.length]);

        self.in_progress.push(Some(old_buffer));
    }

    pub fn recv_loop(&mut self) {

        // Each loop iteration adds to `self.Bytes` and consumes all complete messages.
        // At the start of each iteration, `self.buffer[..self.length]` represents valid
        // data, and the remaining capacity is available for reading from the reader.
        //
        // Once the buffer fills, we need to copy uncomplete messages to a new shared
        // allocation and place the existing Bytes into `self.in_progress`, so that it
        // can be recovered once all readers have read what they need to.

        loop {

            // Attempt to read some more bytes into self.buffer.
            self.length += self.reader.read(&mut self.buffer[self.length ..]).unwrap_or(0);

            // Consume complete messages from the front of self.buffer.
            while let Some(header) = MessageHeader::try_read(&mut &self.buffer[.. self.length]) {
                // TODO: Consolidate message sequences sent to the same worker.
                let peeled_bytes = header.required_bytes();
                let bytes = self.buffer.extract_to(peeled_bytes);
                self.length -= peeled_bytes;
                self.targets[header.target - self.worker_offset].push(bytes);
            }

            // If our buffer is full we should copy it to a new buffer.
            if self.length == self.buffer.len() {
                // If full and not complete, we must increase the size.
                if self.length == self.size {
                    self.size *= 2;
                }
                self.refresh_buffer();
            }
        }
    }
}

// structure in charge of sending data to a Writer, for example the network.
pub struct BinarySender<W: Write> {
    writer:     W,
    sources:    Vec<SharedQueue<Bytes>>,
    log_sender: ::logging::CommsLogger,
}

impl<W: Write> BinarySender<W> {
    pub fn new(writer: W, sources: Vec<SharedQueue<Bytes>>, log_sender: ::logging::CommsLogger) -> BinarySender<W> {
        BinarySender { writer, sources, log_sender }
    }

    pub fn send_loop(&mut self) {

        let mut stash = Vec::new();
        while !self.sources.is_empty() {

            for source in self.sources.iter_mut() {
                source.drain_into(&mut stash);
            }

            // If we got zero data, check that everyone is still alive.
            if stash.is_empty() {
                self.sources.retain(|x| !x.is_done());
                self.writer.flush().expect("Failed to flush writer.");
            }

            for bytes in stash.drain(..) {
                self.writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
            }

        }
    }
}