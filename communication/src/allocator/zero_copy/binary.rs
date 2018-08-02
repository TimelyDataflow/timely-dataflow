use std::io::{Read, Write};
use std::sync::{Arc, mpsc::{Sender, Receiver}};
use std::net::TcpStream;


use bytes::arc::Bytes;

use networking::MessageHeader;

/// Receives serialized data from a `Read`, for example the network.
///
/// The `BinaryReceiver` repeatedly reads binary data from its reader into
/// a binary Bytes slice which can be broken off and handed to recipients as
/// messages become complete.
pub struct BinaryReceiver {

    worker_offset:  usize,

    reader:         TcpStream,                  // the generic reader.
    buffer:         Bytes,                      // current working buffer.
    length:         usize,                      // consumed buffer elements.
    targets:        Vec<Sender<Bytes>>,         // to process-local workers.
    log_sender:     ::logging::CommsLogger,     // logging stuffs.

    in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
    stash:          Vec<Vec<u8>>,               // reclaimed and resuable buffers.
    size:           usize,                      // current buffer allocation size.
}

impl BinaryReceiver {

    pub fn new(
        reader: TcpStream,
        targets: Vec<Sender<Bytes>>,
        worker_offset: usize,
        log_sender: ::logging::CommsLogger) -> BinaryReceiver {
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
                        Ok(mut vec)    => { self.stash.push(vec); },
                        Err(bytes) => { *shared = Some(bytes); },
                    }
                }
            }
            self.in_progress.retain(|x| x.is_some());
        }

        let self_size = self.size;
        self.stash.retain(|x| x.capacity() == self_size);

        let new_buffer = self.stash.pop().unwrap_or_else(|| vec![0; self.size]);
        let new_buffer = Bytes::from(new_buffer);
        let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

        debug_assert!(self.length <= old_buffer.len());
        debug_assert!(self.length <= self.buffer.len());

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

        let mut active = true;
        let mut count = 0;

        while active {

            // Attempt to read some more bytes into self.buffer.
            let read = match self.reader.read(&mut self.buffer[self.length ..]) {
                Ok(n) => n,
                Err(x) => {
                    // We don't expect this, as socket closure results in Ok(0) reads.
                    println!("Error: {:?}", x);
                    0
                },
            };

            active = read > 0;

            self.length += read;

            // Consume complete messages from the front of self.buffer.
            while let Some(header) = MessageHeader::try_read(&mut &self.buffer[.. self.length]) {

                // TODO: Consolidate message sequences sent to the same worker.
                let peeled_bytes = header.required_bytes();
                let bytes = self.buffer.extract_to(peeled_bytes);
                self.length -= peeled_bytes;

                self.targets[header.target - self.worker_offset]
                    .send(bytes)
                    .expect("Worker queue unavailable in recv_loop");
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

        // println!("RECVER EXITING");
    }
}

// impl Drop for BinaryReceiver {
//     fn drop(&mut self) {
//         self.reader.shutdown(::std::net::Shutdown::Read).expect("Read shutdown failed");
//     }
// }

// structure in charge of sending data to a Writer, for example the network.
pub struct BinarySender {
    writer:     ::std::io::BufWriter<TcpStream>,
    source:     Receiver<(Bytes, Arc<()>)>,
    log_sender: ::logging::CommsLogger,
}

impl BinarySender {
    pub fn new(writer: ::std::io::BufWriter<TcpStream>, source: Receiver<(Bytes, Arc<()>)>, log_sender: ::logging::CommsLogger) -> BinarySender {
        BinarySender { writer, source, log_sender }
    }

    pub fn send_loop(&mut self) {

        let mut count = 0;
        let mut stash = Vec::new();

        while let Ok((bytes, _count)) = self.source.recv() {
            stash.push(bytes);
            while let Ok((bytes, _count)) = self.source.try_recv() {
                stash.push(bytes);
            }

            // TODO: Could do scatter/gather write here.
            for bytes in stash.drain(..) {
                count += bytes.len();
                // println!("Sending bytes: {:?}", bytes.len());
                self.writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
                // println!("Sent bytes: {:?}", count);
            }
            // TODO: Could delay this until a self.source.recv() would block.
            self.writer.flush().expect("Failed to flush writer.");
        }

        // println!("SENDER EXITING");
    }
}

impl Drop for BinarySender {
    fn drop(&mut self) {
        self.writer.flush().expect("Failed to flush writer.");
        self.writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    }
}