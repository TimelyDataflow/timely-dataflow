//!

use std::io::Write;
use anyhow::{bail, Context};
use crossbeam_channel::{Sender, Receiver};

use crate::networking::MessageHeader;

use super::bytes_slab::BytesSlab;
use super::bytes_exchange::MergeQueue;
use super::stream::Stream;

use logging_core::Logger;

use crate::logging::{CommunicationEvent, CommunicationSetup, MessageEvent, StateEvent};

/// Repeatedly reads from a TcpStream and carves out messages.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
///
/// If the stream ends without being shut down, or if reading from the stream fails, the
/// receive thread panics with a message that starts with "timely communication error:"
/// in an attempt to take down the computation and cause the failures to cascade.
pub fn recv_loop<S>(
    mut reader: S,
    targets: Vec<Receiver<MergeQueue>>,
    worker_offset: usize,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
-> crate::Result<()>
where
    S: Stream,
{
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: true }));

    let mut targets: Vec<MergeQueue> = targets.into_iter().map(|x| x.recv().expect("Failed to receive MergeQueue")).collect();

    let mut buffer = BytesSlab::new(20);

    // Where we stash Bytes before handing them off.
    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0 .. targets.len() {
        stageds.push(Vec::new());
    }

    let result = recv_loop_inner(&mut reader, worker_offset, &mut logger, &mut targets, &mut buffer, &mut stageds);

    for target in &mut targets {
        target.poison();
    }

    // Log the receive thread's end.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: false, }));
    result
}

fn recv_loop_inner<S>(
    reader: &mut S,
    worker_offset: usize,
    logger: &mut Option<Logger<CommunicationEvent, CommunicationSetup>>,
    targets: &mut Vec<MergeQueue>,
    buffer: &mut BytesSlab,
    stageds: &mut Vec<Vec<bytes::arc::Bytes>>
) -> crate::Result<()>
where
    S: Stream
{
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
        let read = match reader.read(&mut buffer.empty()).context("reading data")? {
            0 => bail!("reading data: Unexpected EOF"),
            n => n,
        };

        buffer.make_valid(read);

        // Consume complete messages from the front of self.buffer.
        while let Some(header) = MessageHeader::try_read(buffer.valid()) {

            // TODO: Consolidate message sequences sent to the same worker?
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            // Record message receipt.
            logger.as_mut().map(|logger| {
                logger.log(MessageEvent { is_send: false, header, });
            });

            if header.length > 0 {
                stageds[header.target - worker_offset].push(bytes);
            }
            else {
                // Shutting down; confirm absence of subsequent data.
                active = false;
                if !buffer.valid().is_empty() {
                    bail!("Clean shutdown followed by data.");
                }
                buffer.ensure_capacity(1);
                if reader.read(&mut buffer.empty()).context("reading data")? > 0 {
                    bail!("Clean shutdown followed by data.");
                }
            }
        }

        // Pass bytes along to targets.
        for (index, staged) in stageds.iter_mut().enumerate() {
            // FIXME: try to merge `staged` before handing it to BytesPush::extend
            use crate::allocator::zero_copy::bytes_exchange::BytesPush;
            targets[index].extend(staged.drain(..))?;
        }
    }
    Ok(())
}

/// Repeatedly sends messages into a TcpStream.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
///
/// If writing to the stream fails, the send thread panics with a message that starts with
/// "timely communication error:" in an attempt to take down the computation and cause the
/// failures to cascade.
pub fn send_loop<S: Stream>(
    // TODO: Maybe we don't need BufWriter with consolidation in writes.
    writer: S,
    sources: Vec<Sender<MergeQueue>>,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>,
) -> crate::Result<()>
{

    // Log the send thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: true, }));

    let mut sources: Vec<Option<MergeQueue>> = sources.into_iter().map(|x| {
        let buzzer = crate::buzzer::Buzzer::new();
        let queue = MergeQueue::new(buzzer);
        x.send(queue.clone()).expect("failed to send MergeQueue");
        Some(queue)
    }).collect();

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    while !sources.is_empty() {

        // TODO: Round-robin better, to release resources fairly when overloaded.
        for source in sources.iter_mut().flat_map(|s| s) {
            use crate::allocator::zero_copy::bytes_exchange::BytesPull;
            source.drain_into(&mut stash)?;
        }

        if stash.is_empty() {
            // No evidence of records to read, but sources not yet empty (at start of loop).
            // We are going to flush our writer (to move buffered data), double check on the
            // sources for emptiness and wait on a signal only if we are sure that there will
            // still be a signal incoming.
            //
            // We could get awoken by more data, a channel closing, or spuriously perhaps.
            writer.flush().context("Flushing writer")?;
            for source in sources.iter_mut() {
                if let Some(s) = source {
                    if s.is_complete()? {
                        *source = None;
                    }
                }
            }
            sources.retain(Option::is_some);
            if !sources.is_empty() {
                std::thread::park();
            }
        }
        else {
            // TODO: Could do scatter/gather write here.
            for mut bytes in stash.drain(..) {

                // Record message sends.
                logger.as_mut().map(|logger| {
                    let mut offset = 0;
                    while let Some(header) = MessageHeader::try_read(&mut bytes[offset..]) {
                        logger.log(MessageEvent { is_send: true, header, });
                        offset += header.required_bytes();
                    }
                });

                writer.write_all(&bytes[..]).context("writing data")?;
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
    header.write_to(&mut writer).context("writing data")?;
    writer.flush().context("flushing writer")?;
    writer.get_mut().shutdown(::std::net::Shutdown::Write).context("Write shutdown failed")?;
    logger.as_mut().map(|logger| logger.log(MessageEvent { is_send: true, header }));

    // Log the send thread's end.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: false, }));
    Ok(())
}
