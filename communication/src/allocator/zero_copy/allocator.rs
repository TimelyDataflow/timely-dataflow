//! Zero-copy allocator based on TCP.
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap, hash_map::Entry};
use std::sync::mpsc::{Sender, Receiver};

use timely_bytes::arc::Bytes;

use crate::networking::MessageHeader;

use crate::{Allocate, Push, Pull};
use crate::allocator::{AllocateBuilder, Exchangeable};
use crate::allocator::canary::Canary;
use crate::allocator::zero_copy::bytes_slab::BytesRefill;
use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue};
use super::push_pull::{Pusher, PullerInner};

/// Builds an instance of a TcpAllocator.
///
/// Builders are required because some of the state in a `TcpAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct TcpBuilder<A: AllocateBuilder> {
    inner:  A,
    index:  usize,                      // number out of peers
    peers:  usize,                      // number of peer allocators.
    futures:   Vec<Receiver<MergeQueue>>,  // to receive queues to each network thread.
    promises:   Vec<Sender<MergeQueue>>,    // to send queues from each network thread.
    /// Byte slab refill function.
    refill: BytesRefill,
}

/// Creates a vector of builders, sharing appropriate state.
///
/// `threads` is the number of workers in a single process, `processes` is the
/// total number of processes.
/// The returned tuple contains
/// ```ignore
/// (
///   AllocateBuilder for local threads,
///   info to spawn egress comm threads,
///   info to spawn ingress comm thresds,
/// )
/// ```
pub fn new_vector<A: AllocateBuilder>(
    allocators: Vec<A>,
    my_process: usize,
    processes: usize,
    refill: BytesRefill,
) -> (Vec<TcpBuilder<A>>,
    Vec<Vec<Sender<MergeQueue>>>,
    Vec<Vec<Receiver<MergeQueue>>>)
{
    let threads = allocators.len();

    // For queues from worker threads to network threads, and vice versa.
    let (network_promises, worker_futures) = crate::promise_futures(processes-1, threads);
    let (worker_promises, network_futures) = crate::promise_futures(threads, processes-1);

    let builders =
    allocators
        .into_iter()
        .zip(worker_promises)
        .zip(worker_futures)
        .enumerate()
        .map(|(index, ((inner, promises), futures))| {
            TcpBuilder {
                inner,
                index: my_process * threads + index,
                peers: threads * processes,
                promises,
                futures,
                refill: refill.clone(),
            }})
        .collect();

    (builders, network_promises, network_futures)
}

impl<A: AllocateBuilder> TcpBuilder<A> {

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> TcpAllocator<A::Allocator> {

        // Fulfill puller obligations.
        let mut recvs = Vec::with_capacity(self.peers);
        for promise in self.promises.into_iter() {
            let buzzer = crate::buzzer::Buzzer::default();
            let queue = MergeQueue::new(buzzer);
            promise.send(queue.clone()).expect("Failed to send MergeQueue");
            recvs.push(queue.clone());
        }

        // Extract pusher commitments.
        let mut sends = Vec::with_capacity(self.peers);
        for pusher in self.futures.into_iter() {
            let queue = pusher.recv().expect("Failed to receive push queue");
            let sendpoint = SendEndpoint::new(queue, self.refill.clone());
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        // let sends: Vec<_> = self.sends.into_iter().map(
        //     |send| Rc::new(RefCell::new(SendEndpoint::new(send)))).collect();

        TcpAllocator {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            canaries: Rc::new(RefCell::new(Vec::new())),
            channel_id_bound: None,
            staged: Vec::new(),
            sends,
            recvs,
            to_local: HashMap::new(),
        }
    }
}

/// A TCP-based allocator for inter-process communication.
pub struct TcpAllocator<A: Allocate> {

    inner:      A,                                  // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).

    staged:     Vec<Bytes>,                         // staging area for incoming Bytes
    canaries:   Rc<RefCell<Vec<usize>>>,

    channel_id_bound: Option<usize>,

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>,     // sends[x] -> goes to process x.
    recvs:      Vec<MergeQueue>,                                // recvs[x] <- from process x.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,   // to worker-local typed pullers.
}

impl<A: Allocate> Allocate for TcpAllocator<A> {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Exchangeable>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {

        // Assume and enforce in-order identifier allocation.
        if let Some(bound) = self.channel_id_bound {
            assert!(bound < identifier);
        }
        self.channel_id_bound = Some(identifier);

        // Result list of boxed pushers.
        let mut pushes = Vec::<Box<dyn Push<T>>>::new();

        // Inner exchange allocations.
        let inner_peers = self.inner.peers();
        let (mut inner_sends, inner_recv) = self.inner.allocate(identifier);

        for target_index in 0 .. self.peers() {

            // TODO: crappy place to hardcode this rule.
            let mut process_id = target_index / inner_peers;

            if process_id == self.index / inner_peers {
                pushes.push(inner_sends.remove(0));
            }
            else {
                // message header template.
                let header = MessageHeader {
                    channel:    identifier,
                    source:     self.index,
                    target_lower:     target_index,
                    target_upper:     target_index + 1,
                    length:     0,
                    seqno:      0,
                };

                // create, box, and stash new process_binary pusher.
                if process_id > self.index / inner_peers { process_id -= 1; }
                pushes.push(Box::new(Pusher::new(header, Rc::clone(&self.sends[process_id]))));
            }
        }

        let channel = Rc::clone(self.to_local.entry(identifier).or_default());

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, Rc::clone(&self.canaries));
        let puller = Box::new(CountPuller::new(PullerInner::new(inner_recv, channel, canary), identifier, Rc::clone(self.events())));

        (pushes, puller, )
    }

    fn broadcast<T: Exchangeable + Clone>(&mut self, identifier: usize) -> (Box<dyn Push<T>>, Box<dyn Pull<T>>) {

        // Assume and enforce in-order identifier allocation.
        if let Some(bound) = self.channel_id_bound {
            assert!(bound < identifier);
        }
        self.channel_id_bound = Some(identifier);

        // Result list of boxed pushers.
        // One entry for each process.
        let mut pushes = Vec::<Box<dyn Push<T>>>::with_capacity(self.sends.len() + 1);

        // Inner exchange allocations.
        let inner_peers = self.inner.peers();
        let (inner_send, inner_recv) = self.inner.broadcast(identifier);

        pushes.push(inner_send);
        for (mut index, send) in self.sends.iter().enumerate() {
            // The span of worker indexes jumps by `inner_peers` as we skip our own process.
            // We bump `index` by one as we pass `self.index/inner_peers` to effect this.
            if index >= self.index/inner_peers { index += 1; }
            let header = MessageHeader {
                channel: identifier,
                source: self.index,
                target_lower: index * inner_peers,
                target_upper: index * inner_peers + inner_peers,
                length: 0,
                seqno: 0,
            };
            pushes.push(Box::new(Pusher::new(header, Rc::clone(send))))
        }

        let channel = Rc::clone(self.to_local.entry(identifier).or_default());

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, Rc::clone(&self.canaries));
        let puller = Box::new(CountPuller::new(PullerInner::new(inner_recv, channel, canary), identifier, Rc::clone(self.events())));

        let pushes = Box::new(crate::allocator::Broadcaster { spare: None, pushers: pushes });
        (pushes, puller, )
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn receive(&mut self) {

        // Check for channels whose `Puller` has been dropped.
        let mut canaries = self.canaries.borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
            self.to_local
                .remove(&dropped_channel)
                .expect("non-existent channel dropped");
            // Borrowed channels may be non-empty, if the dataflow was forcibly
            // dropped. The contract is that if a dataflow is dropped, all other
            // workers will drop the dataflow too, without blocking indefinitely
            // on events from it.
            // assert!(dropped.borrow().is_empty());
        }
        ::std::mem::drop(canaries);

        self.inner.receive();

        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        let mut events = self.inner.events().borrow_mut();

        for mut bytes in self.staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while !bytes.is_empty() {

                if let Some(header) = MessageHeader::try_read(&bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(::std::mem::size_of::<MessageHeader>());

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push(header.channel);

                    // Ensure that a queue exists.
                    match self.to_local.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            if self.channel_id_bound.map(|b| b < header.channel).unwrap_or(true) {
                                entry.insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back(peel);
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back(peel);
                        }
                    }
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
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        self.inner.events()
    }
    fn await_events(&self, duration: Option<std::time::Duration>) {
        self.inner.await_events(duration);
    }
}
