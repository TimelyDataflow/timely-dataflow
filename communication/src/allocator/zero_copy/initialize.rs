//! Network initialization.

use std::sync::Arc;
use timely_logging::Logger;
use crate::allocator::PeerBuilder;
use crate::allocator::zero_copy::bytes_slab::BytesRefill;
use crate::logging::CommunicationEventBuilder;
use crate::networking::create_sockets;
use super::tcp::{send_loop, recv_loop};
use super::allocator::{TcpBuilder, new_vector};
use super::stream::Stream;

/// Join handles for send and receive threads.
///
/// On drop, the guard joins with each of the threads to ensure that they complete
/// cleanly and send all necessary data.
pub struct CommsGuard {
    send_guards: Vec<::std::thread::JoinHandle<()>>,
    recv_guards: Vec<::std::thread::JoinHandle<()>>,
}

impl Drop for CommsGuard {
    fn drop(&mut self) {
        for handle in self.send_guards.drain(..) {
            handle.join().expect("Send thread panic");
        }
        // println!("SEND THREADS JOINED");
        for handle in self.recv_guards.drain(..) {
            handle.join().expect("Recv thread panic");
        }
        // println!("RECV THREADS JOINED");
    }
}

use crate::logging::CommunicationSetup;

/// Initializes network connections
pub fn initialize_networking<P: PeerBuilder>(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    refill: BytesRefill,
    log_sender: Arc<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEventBuilder>>+Send+Sync>,
)
-> ::std::io::Result<(Vec<TcpBuilder<P::Peer>>, CommsGuard)>
{
    let sockets = create_sockets(addresses, my_index, noisy)?;
    initialize_networking_from_sockets::<_, P>(sockets, my_index, threads, refill, log_sender)
}

/// Initialize send and recv threads from sockets.
///
/// This method is available for users who have already connected sockets and simply wish to construct
/// a vector of process-local allocators connected to instantiated send and recv threads.
///
/// It is important that the `sockets` argument contain sockets for each remote process, in order, and
/// with position `my_index` set to `None`.
pub fn initialize_networking_from_sockets<S: Stream + 'static, P: PeerBuilder>(
    mut sockets: Vec<Option<S>>,
    my_index: usize,
    threads: usize,
    refill: BytesRefill,
    log_sender: Arc<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEventBuilder>>+Send+Sync>,
)
-> ::std::io::Result<(Vec<TcpBuilder<P::Peer>>, CommsGuard)>
{
    // Sockets are expected to be blocking,
    for socket in sockets.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }

    let processes = sockets.len();

    let process_allocators = P::new_vector(threads, refill.clone());
    let (builders, promises, futures) = new_vector(process_allocators, my_index, processes, refill.clone());

    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    let mut send_guards = Vec::with_capacity(sockets.len());
    let mut recv_guards = Vec::with_capacity(sockets.len());
    let refill = refill.clone();

    // for each process, if a stream exists (i.e. not local) ...
    for (index, stream) in sockets.into_iter().enumerate().filter_map(|(i, s)| s.map(|s| (i, s))) {
        let remote_recv = promises_iter.next().unwrap();

        {
            let log_sender = Arc::clone(&log_sender);
            let stream = stream.try_clone()?;
            let join_guard =
            ::std::thread::Builder::new()
                .name(format!("timely:send-{}", index))
                .spawn(move || {

                    let logger = log_sender(CommunicationSetup {
                        process: my_index,
                        sender: true,
                        remote: Some(index),
                    });

                    send_loop(stream, remote_recv, my_index, index, logger);
                })?;

            send_guards.push(join_guard);
        }

        let remote_send = futures_iter.next().unwrap();

        {
            // let remote_sends = remote_sends.clone();
            let log_sender = Arc::clone(&log_sender);
            let stream = stream.try_clone()?;
            let refill = refill.clone();
            let join_guard =
            ::std::thread::Builder::new()
                .name(format!("timely:recv-{}", index))
                .spawn(move || {
                    let logger = log_sender(CommunicationSetup {
                        process: my_index,
                        sender: false,
                        remote: Some(index),
                    });
                    recv_loop(stream, remote_send, threads * my_index, my_index, index, refill, logger);
                })?;

            recv_guards.push(join_guard);
        }
    }

    Ok((builders, CommsGuard { send_guards, recv_guards }))
}
