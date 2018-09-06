//!

use std::io::{Read, Write};
use std::net::TcpStream;

use networking::MessageHeader;

use super::bytes_slab::BytesSlab;
use super::bytes_exchange::{MergeQueue, Signal};

use logging_core::Logger;

/// Repeatedly reads from a TcpStream and carves out messages.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
/// If the stream ends without being shut down, the receive thread panics in an attempt to
/// take down the computation and cause the failures to cascade.
pub fn recv_loop(
    mut reader: TcpStream,
    mut targets: Vec<MergeQueue>,
    worker_offset: usize,
    _log_sender: Option<Logger<::log_events::CommunicationEvent>>)
{
    let mut buffer = BytesSlab::new(20);

    // Where we stash Bytes before handing them off.
    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0 .. targets.len() {
        stageds.push(Vec::new());
    }

    // Each loop iteration adds to `self.Bytes` and consumes all complete messages.
    // At the start of each iteration, `self.buffer[..self.length]` represents valid
    // data, and the remaining capacity is available for reading from the reader.
    //
    // Once the buffer fills, we need to copy uncomplete messages to a new shared
    // allocation and place the existing Bytes into `self.in_progress`, so that it
    // can be recovered once all readers have read what they need to.
    let mut active = true;
    while active {

        buffer.ensure_capacity(1);

        assert!(!buffer.empty().is_empty());

        // Attempt to read some more bytes into self.buffer.
        let read = match reader.read(&mut buffer.empty()) {
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            },
        };

        assert!(read > 0);
        buffer.make_valid(read);

        // Consume complete messages from the front of self.buffer.
        while let Some(header) = MessageHeader::try_read(buffer.valid()) {

            // TODO: Consolidate message sequences sent to the same worker?
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            if header.length > 0 {
                stageds[header.target - worker_offset].push(bytes);
            }
            else {
                // Shutting down; confirm absence of subsequent data.
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                buffer.ensure_capacity(1);
                if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                    panic!("Clean shutdown followed by data.");
                }
            }
        }

        // Pass bytes along to targets.
        for (index, staged) in stageds.iter_mut().enumerate() {
            use allocator::zero_copy::bytes_exchange::BytesPush;
            targets[index].extend(staged.drain(..));
        }
    }
    // println!("RECVER EXITING");
}

/// Repeatedly sends messages into a TcpStream.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
pub fn send_loop(
    // TODO: Maybe we don't need BufWriter with consolidation in writes.
    writer: TcpStream,
    mut sources: Vec<MergeQueue>,
    signal: Signal,
    _log_sender: Option<Logger<::log_events::CommunicationEvent>>)
{

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    while !sources.is_empty() {

        // TODO: Round-robin better, to release resources fairly when overloaded.
        for source in sources.iter_mut() {
            use allocator::zero_copy::bytes_exchange::BytesPull;
            source.drain_into(&mut stash);
        }

        if stash.is_empty() {
            // No evidence of records to read, but sources not yet empty (at start of loop).
            // We are going to flush our writer (to move buffered data) and wait on a signal.
            // We could get awoken by more data, a channel closing, or spuriously perhaps.
            writer.flush().expect("Failed to flush writer.");
            signal.wait();
            sources.retain(|source| !source.is_complete());
        }
        else {
            // TODO: Could do scatter/gather write here.
            for bytes in stash.drain(..) {
                writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
            }
        }
    }

    // Write final zero-length header.
    // Would be better with meaningful metadata, but as this stream merges many
    // workers it isn't clear that there is anything specific to write here.
    let header = MessageHeader {
        channel:    0,
        source:     0,
        target:     0,
        length:     0,
        seqno:      0,
    };
    header.write_to(&mut writer).expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
}