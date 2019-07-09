//! Network initialization.

use std::sync::Arc;
// use crate::allocator::Process;
use crate::allocator::process::ProcessBuilder;
use crate::networking::create_sockets;
use super::tcp::{send_loop, recv_loop};
use super::allocator::{TcpBuilder, new_vector};

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

use crate::logging::{CommunicationSetup, CommunicationEvent};
use logging_core::Logger;

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    let log_sender = Arc::new(log_sender);
    let processes = addresses.len();

    // one per process (including local, which would be None)
    let mut results: Vec<Option<::std::net::TcpStream>> =
        create_sockets(addresses, my_index, noisy)?;

    let process_allocators = crate::allocator::process::Process::new_vector(threads);
    let (builders, promises, futures) = new_vector(process_allocators, my_index, processes);

    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    let mut send_guards = Vec::new();
    let mut recv_guards = Vec::new();

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..results.len() {

        if let Some(stream) = results[index].take() {
            // remote process

            let remote_recv = promises_iter.next().unwrap();

            {
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
                let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("send thread {}", index))
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
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
                let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("recv thread {}", index))
                    .spawn(move || {
                        let logger = log_sender(CommunicationSetup {
                            process: my_index,
                            sender: false,
                            remote: Some(index),
                        });
                        recv_loop(stream, remote_send, threads * my_index, my_index, index, logger);
                    })?;

                recv_guards.push(join_guard);
            }

        }
    }

    Ok((builders, CommsGuard { send_guards, recv_guards }))
}