//! Network initialization.

use std::sync::Arc;
use allocator::Process;
use networking::create_sockets;
use super::bytes_exchange::{MergeQueue, Signal};
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

use ::logging::{CommunicationSetup, CommunicationEvent};
use logging_core::Logger;

/// Whether Loop spawns a sender loop or receiver loop
enum SendOrRecv {
    /// Loop invokes send_loop
    Send(Signal),
    /// Loop invokes recv_loop
    Recv,
}

impl SendOrRecv {
    fn is_send(&self) -> bool {
        match self {
            SendOrRecv::Send(_) => true,
            SendOrRecv::Recv => false,
        }
    }
}

/// An opaque handle to invoke a communication's thread send/recv loop logic
///
/// This is equivalent to an `FnOnce` capturing the parameters to send_loop/recv_loop
/// and is necessary because `FnOnce` cannot be called when boxed.
pub struct Loop {
    /// The index of this communication thread
    my_index: usize,
    /// Whether this is a send or recv loop
    send_or_recv: SendOrRecv,
    /// The remote this interacts with
    remote: usize,
    /// Number of threads per worker
    threads: usize,
    /// Tcp stream to read from / write to
    stream: ::std::net::TcpStream,
    /// Local endpoints to read from / write to
    remote_sendrecv: Vec<MergeQueue>,
    /// Function that makes a new logger for this thread
    log_sender: Arc<Box<Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>>,
}

impl Loop {
    /// Start the send/recv loop
    pub fn start(self) {
        let logger = (self.log_sender)(CommunicationSetup {
            process: self.my_index,
            sender: self.send_or_recv.is_send(),
            remote: Some(self.remote),
        });
        match self.send_or_recv {
            SendOrRecv::Send(signal) => send_loop(
                self.stream,
                self.remote_sendrecv,
                signal,
                self.my_index,
                self.remote,
                logger),
            SendOrRecv::Recv => recv_loop(
                self.stream,
                self.remote_sendrecv,
                self.threads * self.my_index,
                self.my_index,
                self.remote,
                logger),
        }
    }
}

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    spawn_fn: Box<Fn(/* index: */ usize, /* sender: */ bool, /* remote: */ Option<usize>, Loop)->()+Send+Sync>,
    log_sender: Box<Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder<Process>>, CommsGuard)>
// where
//     F: Fn(CommunicationSetup)->Option<Logger<CommunicationEvent>>+Send+Sync+'static,
{
    let log_sender = Arc::new(log_sender);
    let processes = addresses.len();

    let mut results = create_sockets(addresses, my_index, noisy)?;

    let (builders, remote_recvs, remote_sends) = new_vector(my_index, threads, processes);
    let mut remote_recv_iter = remote_recvs.into_iter();
    let mut remote_send_iter = remote_sends.into_iter();

    let mut send_guards = Vec::new();
    let mut recv_guards = Vec::new();

    let spawn_fn = Arc::new(spawn_fn);

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..results.len() {

        if let Some(stream) = results[index].take() {

            let (remote_recv, signal) = remote_recv_iter.next().unwrap();

            {
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;

                let spawn_fn = spawn_fn.clone();
                let join_guard =
                    ::std::thread::Builder::new()
                        .name(format!("send thread {}", index))
                        .spawn(move || spawn_fn(my_index, true, Some(index), Loop {
                            threads,
                            my_index,
                            send_or_recv: SendOrRecv::Send(signal),
                            remote: index,
                            stream,
                            remote_sendrecv: remote_recv,
                            log_sender,
                        }))?;

                send_guards.push(join_guard);
            }

            let remote_send = remote_send_iter.next().unwrap();

            {
                // let remote_sends = remote_sends.clone();
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
                let spawn_fn = spawn_fn.clone();
                let join_guard =
                    ::std::thread::Builder::new()
                        .name(format!("recv thread {}", index))
                        .spawn(move || spawn_fn(my_index, false, Some(index), Loop {
                            threads,
                            my_index,
                            send_or_recv: SendOrRecv::Recv,
                            remote: index,
                            stream,
                            remote_sendrecv: remote_send,
                            log_sender,
                        }))?;

                recv_guards.push(join_guard);
            }

        }
    }

    Ok((builders, CommsGuard { send_guards, recv_guards }))
}
