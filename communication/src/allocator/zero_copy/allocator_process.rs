//! Zero-copy allocator for intra-process serialized communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::arc::Bytes;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull};
use allocator::Message;

use super::bytes_exchange::{BytesPull, SendEndpoint};

use super::push_pull::{Pusher, Puller};

/// Builds an instance of a ProcessAllocator.
///
/// Builders are required because some of the state in a `ProcessAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct ProcessBuilder {
    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators.
    sends:      Vec<Arc<Mutex<VecDeque<Bytes>>>>,   // for pushing bytes at remote processes.
    recvs:      Vec<Arc<Mutex<VecDeque<Bytes>>>>,   // for pulling bytes from remote processes.
}

impl ProcessBuilder {
    /// Creates a vector of builders, sharing appropriate state.
    ///
    /// This method requires access to a byte exchanger, from which it mints channels.
    pub fn new_vector(count: usize) -> Vec<ProcessBuilder> {

        let mut sends = Vec::new();
        let mut recvs = Vec::new();
        for _ in 0 .. count { sends.push(Vec::new()); }
        for _ in 0 .. count { recvs.push(Vec::new()); }

        for source in 0 .. count {
            for target in 0 .. count {
                let send = Arc::new(Mutex::new(VecDeque::new()));
                let recv = send.clone();
                sends[source].push(send);
                recvs[target].push(recv);
            }
        }

        let mut result = Vec::new();
        for (index, (sends, recvs)) in sends.drain(..).zip(recvs.drain(..)).enumerate() {
            result.push(ProcessBuilder {
                index,
                peers: count,
                sends,
                recvs,
            })
        }

        result
    }

    /// Builds a `ProcessAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> ProcessAllocator {

        let mut sends = Vec::new();
        for send in self.sends.into_iter() {
            let sendpoint = SendEndpoint::new(send);
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        ProcessAllocator {
            // inner: self.inner,
            index: self.index,
            peers: self.peers,
            allocated: 0,
            sends,
            recvs: self.recvs,
            to_local: Vec::new(),
        }
    }
}

/// A serializing allocator for inter-thread intra-process communication.
pub struct ProcessAllocator {

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    allocated:  usize,                              // indicates how many channels have been allocated (locally).

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<Arc<Mutex<VecDeque<Bytes>>>>>>>, // sends[x] -> goes to process x.
    recvs:      Vec<Arc<Mutex<VecDeque<Bytes>>>>,                            // recvs[x] <- from process x?.
    to_local:   Vec<Rc<RefCell<VecDeque<Bytes>>>>,  // to worker-local typed pullers.
}

impl Allocate for ProcessAllocator {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>, Option<usize>) {

        let channel_id = self.allocated;
        self.allocated += 1;

        let mut pushes = Vec::<Box<Push<Message<T>>>>::new();

        for target_index in 0 .. self.peers() {

            // message header template.
            let header = MessageHeader {
                channel:    channel_id,
                source:     self.index,
                target:     target_index,
                length:     0,
                seqno:      0,
            };

            // create, box, and stash new process_binary pusher.
            pushes.push(Box::new(Pusher::new(header, self.sends[target_index].clone())));
        }

        while self.to_local.len() <= channel_id {
            self.to_local.push(Rc::new(RefCell::new(VecDeque::new())));
        }

        let puller = Box::new(Puller::new(self.to_local[channel_id].clone()));

        (pushes, puller, None)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn pre_work(&mut self) {

        for recv in self.recvs.iter_mut() {
            while let Some(mut bytes) = recv.pull() {

                // We expect that `bytes` contains an integral number of messages.
                // No splitting occurs across allocations.
                while bytes.len() > 0 {

                    if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

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