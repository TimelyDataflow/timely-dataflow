//! Zero-copy allocator for intra-process serialized communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::sync::mpsc::{Sender, Receiver};

use bytes::arc::Bytes;

use crate::networking::MessageHeader;

use crate::{Allocate, Message, Data, Push, Pull};
use crate::allocator::{AllocateBuilder, Event};
use crate::allocator::canary::Canary;

use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue};

use super::push_pull::{Pusher, Puller};

/// Builds an instance of a ProcessAllocator.
///
/// Builders are required because some of the state in a `ProcessAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct ProcessBuilder {
    index:  usize,                      // number out of peers
    peers:  usize,                      // number of peer allocators.
    pushers: Vec<Receiver<MergeQueue>>, // for pushing bytes at other workers.
    pullers: Vec<Sender<MergeQueue>>,   // for pulling bytes from other workers.
    // signal:  Signal,
}

impl ProcessBuilder {
    /// Creates a vector of builders, sharing appropriate state.
    ///
    /// This method requires access to a byte exchanger, from which it mints channels.
    pub fn new_vector(count: usize) -> Vec<ProcessBuilder> {

        // Channels for the exchange of `MergeQueue` endpoints.
        let (pullers_vec, pushers_vec) = crate::promise_futures(count, count);

        pushers_vec
            .into_iter()
            .zip(pullers_vec)
            .enumerate()
            .map(|(index, (pushers, pullers))|
                ProcessBuilder {
                    index,
                    peers: count,
                    pushers,
                    pullers,
                }
            )
            .collect()
    }

    /// Builds a `ProcessAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> ProcessAllocator {

        // Fulfill puller obligations.
        let mut recvs = Vec::with_capacity(self.peers);
        for puller in self.pullers.into_iter() {
            let buzzer = crate::buzzer::Buzzer::new();
            let queue = MergeQueue::new(buzzer);
            puller.send(queue.clone()).expect("Failed to send MergeQueue");
            recvs.push(queue.clone());
        }

        // Extract pusher commitments.
        let mut sends = Vec::with_capacity(self.peers);
        for pusher in self.pushers.into_iter() {
            let queue = pusher.recv().expect("Failed to receive MergeQueue");
            let sendpoint = SendEndpoint::new(queue);
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        ProcessAllocator {
            index: self.index,
            peers: self.peers,
            events: Rc::new(RefCell::new(VecDeque::new())),
            canaries: Rc::new(RefCell::new(Vec::new())),
            staged: Vec::new(),
            sends,
            recvs,
            to_local: HashMap::new(),
            // _signal: self.signal,
        }
    }
}

impl AllocateBuilder for ProcessBuilder {
    type Allocator = ProcessAllocator;
    /// Builds allocator, consumes self.
    fn build(self) -> Self::Allocator {
        self.build()
    }

}

/// A serializing allocator for inter-thread intra-process communication.
pub struct ProcessAllocator {

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).

    events: Rc<RefCell<VecDeque<(usize, Event)>>>,

    canaries: Rc<RefCell<Vec<usize>>>,

    // _signal:     Signal,
    // sending, receiving, and responding to binary buffers.
    staged:     Vec<Bytes>,
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>, // sends[x] -> goes to thread x.
    recvs:      Vec<MergeQueue>,                            // recvs[x] <- from thread x.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,          // to worker-local typed pullers.
}

impl Allocate for ProcessAllocator {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {

        let mut pushes = Vec::<Box<Push<Message<T>>>>::new();

        for target_index in 0 .. self.peers() {

            // message header template.
            let header = MessageHeader {
                channel:    identifier,
                source:     self.index,
                target:     target_index,
                length:     0,
                seqno:      0,
            };

            // create, box, and stash new process_binary pusher.
            pushes.push(Box::new(Pusher::new(header, self.sends[target_index].clone())));
        }

        let channel =
        self.to_local
            .entry(identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, self.canaries.clone());
        let puller = Box::new(CountPuller::new(Puller::new(channel, canary), identifier, self.events().clone()));

        (pushes, puller)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn receive(&mut self) {

        // Check for channels whose `Puller` has been dropped.
        let mut canaries = self.canaries.borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let dropped =
            self.to_local
                .remove(&dropped_channel)
                .expect("non-existent channel dropped");
            assert!(dropped.borrow().is_empty());
        }
        std::mem::drop(canaries);

        let mut events = self.events.borrow_mut();

        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        for mut bytes in self.staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(40);

                    // Increment message count for channel.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    // We may receive data before allocating, and shouldn't block.
                    self.to_local
                        .entry(header.channel)
                        .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                        .borrow_mut()
                        .push_back(peel);
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self) {
        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        // OPTIONAL: Tattle on channels sitting on borrowed data.
        // OPTIONAL: Perhaps copy borrowed data into owned allocation.
        // for (index, list) in self.to_local.iter() {
        //     let len = list.borrow_mut().len();
        //     if len > 0 {
        //         eprintln!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
        //     }
        // }
    }

    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        &self.events
    }
    fn await_events(&self, duration: Option<std::time::Duration>) {
        if self.events.borrow().is_empty() {
            if let Some(duration) = duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}