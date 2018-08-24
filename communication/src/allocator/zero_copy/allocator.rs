use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::mpsc::{channel, Sender, Receiver};

use bytes::arc::Bytes;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull};
use allocator::{Message, Process};

use super::bytes_exchange::{BytesPull, SendEndpoint};
use super::push_pull::{Pusher, PullerInner};

/// Builds an instance of a TcpAllocator.
///
/// Builders are required because some of the state in a `TcpAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct TcpBuilder {
    inner:      Process,
    index:      usize,                          // number out of peers
    peers:      usize,                          // number of peer allocators.
    sends:      Vec<Sender<Bytes>>,  // for pushing bytes at remote processes.
    recvs:      Receiver<Bytes>,                // for pulling bytes from remote processes.
}

impl TcpBuilder {
    /// Creates a vector of builders, sharing appropriate state.
    ///
    /// This method requires access to a byte exchanger, from which it mints channels.
    pub fn new_vector(
        my_process: usize,
        threads: usize,
        processes: usize) -> (Vec<TcpBuilder>, Vec<Receiver<Bytes>>, Vec<Sender<Bytes>>) {

        let mut l2r_send = Vec::new();
        let mut l2r_recv = Vec::new();
        let mut r2l_send = Vec::new();
        let mut r2l_recv = Vec::new();

        for _ in 0 .. threads {
            let (send, recv) = channel();
            r2l_send.push(send);
            r2l_recv.push(recv);
        }

        for _ in 0 .. processes - 1 {
            let (send, recv) = channel();
            l2r_send.push(send);
            l2r_recv.push(recv);
        }

        let builders =
        Process::new_vector(threads)
            .into_iter()
            .zip(r2l_recv.into_iter())
            .enumerate()
            .map(|(index, (inner, recvs))| {
                TcpBuilder {
                    inner,
                    index: my_process * threads + index,
                    peers: threads * processes,
                    sends: l2r_send.clone(),
                    recvs,
                }})
            .collect();

        (builders, l2r_recv, r2l_send)
    }

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> TcpAllocator {

        let mut sends = Vec::new();
        for send in self.sends.into_iter() {
            let sendpoint = SendEndpoint::new(send);
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        TcpAllocator {
            inner: self.inner,
            index: self.index,
            peers: self.peers,
            allocated: 0,
            sends,
            recvs: self.recvs,
            to_local: Vec::new(),
        }
    }
}

// A specific Communicator for inter-thread intra-process communication
pub struct TcpAllocator {

    inner:      Process,                            // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    allocated:  usize,                              // indicates how many channels have been allocated (locally).

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<Sender<Bytes>>>>>,         // sends[x] -> goes to process x.
    recvs:      Receiver<Bytes>,                    // recvs[x] <- from process x?.
    to_local:   Vec<Rc<RefCell<VecDeque<Bytes>>>>,  // to worker-local typed pullers.
}

impl Allocate for TcpAllocator {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>, Option<usize>) {

        let channel_id = self.allocated;
        self.allocated += 1;

        // Result list of boxed pushers.
        let mut pushes = Vec::<Box<Push<Message<T>>>>::new();

        // Inner exchange allocations.
        let inner_peers = self.inner.peers();
        let (mut inner_sends, inner_recv, _) = self.inner.allocate();

        for target_index in 0 .. self.peers() {

            // TODO: crappy place to hardcode this rule.
            let mut process_id = target_index / inner_peers;

            if process_id == self.index / inner_peers {
                pushes.push(inner_sends.remove(0));
            }
            else {
                // message header template.
                let header = MessageHeader {
                    channel:    channel_id,
                    source:     self.index,
                    target:     target_index,
                    length:     0,
                    seqno:      0,
                };

                // create, box, and stash new process_binary pusher.
                if process_id > self.index / inner_peers { process_id -= 1; }
                pushes.push(Box::new(Pusher::new(header, self.sends[process_id].clone())));
            }
        }

        while self.to_local.len() <= channel_id {
            self.to_local.push(Rc::new(RefCell::new(VecDeque::new())));
        }

        let puller = Box::new(PullerInner::new(inner_recv, self.to_local[channel_id].clone()));

        (pushes, puller, None)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn pre_work(&mut self) {

        while let Some(mut bytes) = self.recvs.pull() {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = MessageHeader::try_read(&mut &bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(40);

                    // Ensure that a queue exists.
                    // We may receive data before allocating, and shouldn't block.
                    while self.to_local.len() <= header.channel {
                        self.to_local.push(Rc::new(RefCell::new(VecDeque::new())));
                    }

                    // Introduce the binary slice into the operator input queue.
                    self.to_local[header.channel].borrow_mut().push_back(peel);
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn post_work(&mut self) {
        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        // OPTIONAL: Tattle on channels sitting on borrowed data.
        // OPTIONAL: Perhaps copy borrowed data into owned allocation.
        // for index in 0 .. self.to_local.len() {
        //     let len = self.to_local[index].borrow_mut().len();
        //     if len > 0 {
        //         eprintln!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
        //     }
        // }
    }
}