
use std::sync::Arc;

use super::binary::{allocate_queue_pairs, BinarySender, BinaryReceiver, TcpBytesExchange};
use super::allocator::ProcessBinaryBuilder;

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    log_sender: Arc<Fn(::logging::CommsSetup)->::logging::CommsLogger+Send+Sync>)
-> ::std::io::Result<Vec<ProcessBinaryBuilder<TcpBytesExchange>>> {

    let processes = addresses.len();

    use networking::create_sockets;
    let mut results = create_sockets(addresses, my_index, noisy)?;

    // Send and recv connections between local workers and remote processes.
    let (local_send, remote_recv) = allocate_queue_pairs(threads, results.len() - 1);
    let (local_recv, remote_send) = allocate_queue_pairs(threads, results.len() - 1);

    let mut remote_recv_iter = remote_recv.into_iter();
    let mut remote_send_iter = remote_send.into_iter();

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..results.len() {

        if let Some(stream) = results[index].take() {

            let remote_recv = remote_recv_iter.next().unwrap();
            let remote_send = remote_send_iter.next().unwrap();

            {
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
                // start senders and receivers associated with this stream
                let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("send thread {}", index))
                    .spawn(move || {

                        let log_sender = log_sender(::logging::CommsSetup {
                            process: my_index,
                            sender: true,
                            remote: Some(index),
                        });

                        let stream = ::std::io::BufWriter::with_capacity(1 << 20, stream);
                        BinarySender::new(stream, remote_recv, log_sender)
                            .send_loop()
                    })?;

                // Forget the guard, so that the send thread is not detached from the main thread.
                // This ensures that main thread awaits the completion of the send thread, and all
                // of its transmissions, before exiting and potentially stranding other workers.
                ::std::mem::forget(join_guard);
            }

            {
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
                let _join_guard =
                ::std::thread::Builder::new()
                    .name(format!("recv thread {}", index))
                    .spawn(move || {
                        let log_sender = log_sender(::logging::CommsSetup {
                            process: my_index,
                            sender: false,
                            remote: Some(index),
                        });
                        BinaryReceiver::new(stream, remote_send, threads * my_index, log_sender)
                            .recv_loop()
                    })?;

                // We do not mem::forget the join_guard here, because we deem there to be no harm
                // in closing the process and abandoning the receiver thread. All worker threads
                // will have exited, and we don't expect that continuing to read has a benefit.
                // We could introduce a "shutdown" message into the "protocol" which would confirm
                // a clear conclusion to the interaction.
            }

        }
    }

    let byte_exchange = TcpBytesExchange::new(local_send, local_recv);
    let builders = ProcessBinaryBuilder::new_vector(byte_exchange, my_index, threads, processes);

    Ok(builders)
}