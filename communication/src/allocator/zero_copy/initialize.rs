
use std::sync::Arc;

use super::binary::{BinarySender, BinaryReceiver};
use super::allocator::TcpBuilder;

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

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    log_sender: Arc<Fn(::logging::CommsSetup)->::logging::CommsLogger+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder>, CommsGuard)> {

    let processes = addresses.len();

    use networking::create_sockets;
    let mut results = create_sockets(addresses, my_index, noisy)?;

    let (builders, remote_recvs, remote_sends) = TcpBuilder::new_vector(my_index, threads, processes);
    let mut remote_recv_iter = remote_recvs.into_iter();

    let mut send_guards = Vec::new();
    let mut recv_guards = Vec::new();

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..results.len() {

        if let Some(stream) = results[index].take() {

            let remote_recv = remote_recv_iter.next().unwrap();

            {
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
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

                send_guards.push(join_guard);
            }

            {
                let remote_sends = remote_sends.clone();
                let log_sender = log_sender.clone();
                let stream = stream.try_clone()?;
                let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("recv thread {}", index))
                    .spawn(move || {
                        let log_sender = log_sender(::logging::CommsSetup {
                            process: my_index,
                            sender: false,
                            remote: Some(index),
                        });
                        BinaryReceiver::new(stream, remote_sends, threads * my_index, log_sender)
                            .recv_loop()
                    })?;

                recv_guards.push(join_guard);
            }

        }
    }

    Ok((builders, CommsGuard { send_guards, recv_guards }))
}