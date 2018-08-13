use std::io::{Read, Write};
use std::sync::mpsc::{Sender, Receiver};
use std::net::TcpStream;

use bytes::arc::Bytes;

use networking::MessageHeader;

use super::bytes_slab::BytesSlab;

/// Receives serialized data from a `Read`, for example the network.
///
/// The `BinaryReceiver` repeatedly reads binary data from its reader into
/// a binary Bytes slice which can be broken off and handed to recipients as
/// messages become complete.
pub struct BinaryReceiver {

    worker_offset:  usize,

    reader:         TcpStream,                  // the generic reader.
    targets:        Vec<Sender<Bytes>>,         // to process-local workers.
    log_sender:     ::logging::CommsLogger,     // logging stuffs.

    buffer:         BytesSlab,
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
            worker_offset,
            buffer: BytesSlab::new(20),
        }
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
        while active {

            self.buffer.ensure_capacity(1);

            assert!(!self.buffer.empty().is_empty());

            // Attempt to read some more bytes into self.buffer.
            let read = match self.reader.read(&mut self.buffer.empty()) {
                Ok(n) => n,
                Err(x) => {
                    // We don't expect this, as socket closure results in Ok(0) reads.
                    println!("Error: {:?}", x);
                    0
                },
            };

            active = read > 0;
            self.buffer.make_valid(read);

            // Consume complete messages from the front of self.buffer.
            while let Some(header) = MessageHeader::try_read(&mut self.buffer.valid()) {

                // TODO: Consolidate message sequences sent to the same worker.
                let peeled_bytes = header.required_bytes();
                let bytes = self.buffer.extract(peeled_bytes);

                self.targets[header.target - self.worker_offset]
                    .send(bytes)
                    .expect("Worker queue unavailable in recv_loop");
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
    source:     Receiver<Bytes>,
    log_sender: ::logging::CommsLogger,
}

impl BinarySender {
    pub fn new(writer: ::std::io::BufWriter<TcpStream>, source: Receiver<Bytes>, log_sender: ::logging::CommsLogger) -> BinarySender {
        BinarySender { writer, source, log_sender }
    }

    pub fn send_loop(&mut self) {

        let mut stash = Vec::new();

        while let Ok(bytes) = self.source.recv() {
            stash.push(bytes);
            while let Ok(bytes) = self.source.try_recv() {
                stash.push(bytes);
            }

            // TODO: Could do scatter/gather write here.
            for bytes in stash.drain(..) {
                self.writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
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