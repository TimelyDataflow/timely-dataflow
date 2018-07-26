use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::DerefMut;

use bytes::arc::Bytes;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull};
use allocator::{Message, Process};

use std::sync::{Arc, Mutex};

pub struct SharedQueue<T> {
    queue: Arc<Mutex<VecDeque<T>>>
}

impl<T> SharedQueue<T> {
    pub fn push(&mut self, bytes: T) { self.queue.lock().expect("unable to lock shared queue").push_back(bytes) }
    pub fn pop(&mut self) -> Option<T> { self.queue.lock().expect("unable to lock shared queue").pop_front() }
    pub fn is_empty(&self) -> bool { self.queue.lock().expect("unable to lock shared queue").is_empty() }
    pub fn is_done(&self) -> bool { Arc::strong_count(&self.queue) == 1 }
    pub fn new() -> Self { SharedQueue { queue: Arc::new(Mutex::new(VecDeque::new())) } }
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        SharedQueue { queue: self.queue.clone() }
    }
}

/// A type that can allocate send and receive endpoints for byte exchanges.
///
/// The `BytesExchange` intent is that one can abstractly define mechanisms for exchanging
/// bytes between various entities. In some cases this may be between worker threads within
/// a process, in other cases it may be between worker threads and remote processes. At the
/// moment the cardinalities of remote endpoints requires some context and interpretation.
pub trait BytesExchange {
    /// The type of the send endpoint.
    type Send: SendEndpoint+'static;
    /// The type of the receive endpoint.
    type Recv: RecvEndpoint+'static;
    /// Allocates endpoint pairs for a specified worker.
    ///
    /// Importantly, the Send side may share state to coalesce the buffering and
    /// transmission of records. That is why there are `Rc<RefCell<_>>` things there.
    fn new(&mut self, worker: usize) -> (Vec<Self::Send>, Vec<Self::Recv>);
}

/// A type that can provide and publish writeable binary buffers.
pub trait SendEndpoint {
    /// The type of the writeable binary buffer.
    type SendBuffer: ::std::io::Write;
    /// Provides a writeable buffer of the requested capacity.
    fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer;
    /// Indicates that it is now appropriate to publish the buffer.
    fn publish(&mut self);
}

/// A type that can provide readable binary buffers.
pub trait RecvEndpoint {
    type RecvBuffer: DerefMut<Target=[u8]>;
    /// Provides a readable buffer.
    fn receive(&mut self) -> Option<Self::RecvBuffer>;
}

pub mod common {

    use bytes::arc::Bytes;
    use super::{SendEndpoint, RecvEndpoint, SharedQueue};

    pub struct VecSendEndpoint {
        send: SharedQueue<Bytes>,
        in_progress: Vec<Option<Bytes>>,
        buffer: Vec<u8>,
        stash: Vec<Vec<u8>>,
        default_size: usize,
    }

    impl VecSendEndpoint {
        /// Attempts to recover in-use buffers once uniquely owned.
        fn harvest_shared(&mut self) {
            for shared in self.in_progress.iter_mut() {
                if let Some(bytes) = shared.take() {
                    match bytes.try_recover::<Vec<u8>>() {
                        Ok(vec)    => { self.stash.push(vec); },
                        Err(bytes) => { *shared = Some(bytes); },
                    }
                }
            }
            self.in_progress.retain(|x| x.is_some());
        }

        /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
        fn send_buffer(&mut self) {

            let buffer = ::std::mem::replace(&mut self.buffer, Vec::new());
            let buffer_len = buffer.len();
            if buffer_len > 0 {

                let mut bytes = Bytes::from(buffer);
                let to_send = bytes.extract_to(buffer_len);

                self.send.push(to_send);
                self.in_progress.push(Some(bytes));
            }
            else {
                if buffer.capacity() == self.default_size {
                    self.stash.push(buffer);
                }
            }
        }

        /// Allocates a new `VecSendEndpoint` from a shared queue.
        pub fn new(queue: SharedQueue<Bytes>) -> Self {
            VecSendEndpoint {
                send: queue,
                in_progress: Vec::new(),
                buffer: Vec::new(),
                stash: Vec::new(),
                default_size: 1 << 20,
            }
        }
    }

    impl SendEndpoint for VecSendEndpoint {

        type SendBuffer = Vec<u8>;

        fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer {

            // If we don't have enough capacity in `self.buffer`...
            if self.buffer.capacity() < capacity + self.buffer.len() {
                self.send_buffer();
                if capacity > self.default_size {
                    self.buffer = Vec::with_capacity(capacity);
                }
                else {
                    if self.stash.is_empty() {
                        // Attempt to recover shared buffers.
                        self.harvest_shared();
                    }
                    self.buffer = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(self.default_size))
                }
            }

            &mut self.buffer
        }

        fn publish(&mut self) {
            self.harvest_shared();
            if self.send.is_empty() {
                self.send_buffer();
            }
        }
    }

    pub struct VecRecvEndpoint {
        recv: SharedQueue<Bytes>,
    }


    impl VecRecvEndpoint {
        pub fn new(queue: SharedQueue<Bytes>) -> Self {
            VecRecvEndpoint { recv: queue }
        }
    }

    impl RecvEndpoint for VecRecvEndpoint {
        type RecvBuffer = Bytes;
        fn receive(&mut self) -> Option<Bytes> {
            self.recv.pop()
        }
    }
}

/// Byte exchange mechanisms which use shared memory queues.
pub mod local {

    use bytes::arc::Bytes;

    use super::{BytesExchange, SharedQueue};
    use super::common::{VecSendEndpoint, VecRecvEndpoint};

    pub struct LocalBytesExchange {
        // forward[i][j] contains a shared queue for data from i to j.
        forward: Vec<Vec<SharedQueue<Bytes>>>,
        counter: usize,
    }

    impl LocalBytesExchange {
        fn new(workers: usize) -> LocalBytesExchange {

            let mut forward = Vec::new();
            for _source in 0 .. workers {
                let mut temp_forward = Vec::new();
                for _target in 0 .. workers {
                    temp_forward.push(SharedQueue::new());
                }
                forward.push(temp_forward);
            }

            LocalBytesExchange {
                forward,
                counter: 0,
            }
        }
    }

    impl BytesExchange for LocalBytesExchange {
        type Send = VecSendEndpoint;
        type Recv = VecRecvEndpoint;
        fn new(&mut self, worker: usize) -> (Vec<Self::Send>, Vec<Self::Recv>) {

            let mut sends = Vec::with_capacity(self.forward.len());

            for forward in self.forward[self.counter].iter() {
                sends.push(VecSendEndpoint::new(forward.clone()));
            }

            let mut recvs = Vec::with_capacity(self.forward.len());

            for forward in self.forward.iter() {
                recvs.push(VecRecvEndpoint::new(forward[self.counter].clone()));
            }

            self.counter += 1;

            (sends, recvs)
        }
    }
}

/// A BytesExchange implementation using communication threads and TCP connections.
pub mod tcp {

    use std::io::{Read, Write};

    use bytes::arc::Bytes;

    use networking::MessageHeader;
    use super::{BytesExchange, SharedQueue};
    use super::common::{VecSendEndpoint, VecRecvEndpoint};

    /// Allocates pairs of byte exchanges for remote workers.
    pub struct TcpBytesExchange {
        /// Forward[i,j]: from worker i to process j.
        forward: Vec<Vec<SharedQueue<Bytes>>>,
        /// Reverse[i,j]: to process i from worker j.
        reverse: Vec<Vec<SharedQueue<Bytes>>>,
        counter: usize,
    }

    impl BytesExchange for TcpBytesExchange {

        type Send = VecSendEndpoint;
        type Recv = VecRecvEndpoint;

        // Returns two vectors of length #processes - 1.
        // The first contains destinations to send to remote processes,
        // The second contains sources to receive from remote processes.
        fn new(&mut self, worker: usize) -> (Vec<Self::Send>, Vec<Self::Recv>) {

            let mut sends = Vec::with_capacity(self.forward.len());
            for queue in self.forward[self.counter].iter() {
                sends.push(VecSendEndpoint::new(queue.clone()));
            }

            let mut recvs = Vec::with_capacity(self.forward.len());
            for queue in self.reverse[self.counter].iter() {
                recvs.push(VecRecvEndpoint::new(queue.clone()));
            }

            self.counter += 1;

            (sends, recvs)
        }
    }

    impl TcpBytesExchange {
        fn new() -> Self {
            unimplemented!()
        }
    }

    // Allocates local and remote queue pairs, respectively.
    fn allocate_queue_pairs(local: usize, remote: usize) -> (Vec<Vec<SharedQueue<Bytes>>>, Vec<Vec<SharedQueue<Bytes>>>) {

        // type annotations necessary despite return signature because ... Rust.
        let local_to_remote: Vec<Vec<_>> = (0 .. local).map(|_| (0 .. remote).map(|_| SharedQueue::new()).collect()).collect();
        let remote_to_local: Vec<Vec<_>> = (0 .. remote).map(|r| (0 .. local).map(|l| local_to_remote[l][r].clone()).collect()).collect();

        (local_to_remote, remote_to_local)
    }

    /// Receives serialized data from a `Read`, for example the network.
    ///
    /// The `BinaryReceiver` repeatedly reads binary data from its reader into
    /// a binary Bytes slice which can be broken off and handed to recipients as
    /// messages become complete.
    struct BinaryReceiver<R: Read> {

        worker_offset:  usize,

        reader:         R,                          // the generic reader.
        buffer:         Bytes,                      // current working buffer.
        length:         usize,                      // consumed buffer elements.
        targets:        Vec<SharedQueue<Bytes>>,    // to process-local workers.
        log_sender:     ::logging::CommsLogger,     // logging stuffs.

        in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
        stash:          Vec<Vec<u8>>,               // reclaimed and resuable buffers.
        size:           usize,                      // current buffer allocation size.
    }

    impl<R: Read> BinaryReceiver<R> {

        fn new(
            reader: R,
            targets: Vec<SharedQueue<Bytes>>,
            worker_offset: usize,
            log_sender: ::logging::CommsLogger) -> BinaryReceiver<R> {
            BinaryReceiver {
                reader,
                targets,
                log_sender,
                buffer: Bytes::from(vec![0u8; 1 << 20]),
                length: 0,
                in_progress: Vec::new(),
                stash: Vec::new(),
                size: 1 << 20,
                worker_offset,
            }
        }

        // Retire `self.buffer` and acquire a new buffer of at least `self.size` bytes.
        fn refresh_buffer(&mut self) {

            if self.stash.is_empty() {
                for shared in self.in_progress.iter_mut() {
                    if let Some(bytes) = shared.take() {
                        match bytes.try_recover::<Vec<u8>>() {
                            Ok(vec)    => { self.stash.push(vec); },
                            Err(bytes) => { *shared = Some(bytes); },
                        }
                    }
                }
                self.in_progress.retain(|x| x.is_some());
            }

            let self_size = self.size;
            self.stash.retain(|x| x.capacity() == self_size);


            let new_buffer = self.stash.pop().unwrap_or_else(|| vec![0; 1 << self.size]);
            let new_buffer = Bytes::from(new_buffer);
            let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

            self.buffer[.. self.length].copy_from_slice(&old_buffer[.. self.length]);

            self.in_progress.push(Some(old_buffer));
        }

        fn recv_loop(&mut self) {

            // Each loop iteration adds to `self.Bytes` and consumes all complete messages.
            // At the start of each iteration, `self.buffer[..self.length]` represents valid
            // data, and the remaining capacity is available for reading from the reader.
            //
            // Once the buffer fills, we need to copy uncomplete messages to a new shared
            // allocation and place the existing Bytes into `self.in_progress`, so that it
            // can be recovered once all readers have read what they need to.

            loop {

                // If our buffer is full we should copy it to a new buffer.
                if self.length == self.buffer.len() {
                    // If full and not complete, we must increase the size.
                    if self.length == self.size {
                        self.size *= 2;
                    }
                    self.refresh_buffer();
                }

                // Attempt to read some more bytes into self.buffer.
                self.length = self.reader.read(&mut self.buffer[self.length ..]).unwrap_or(0);

                // Consume complete messages from the front of self.buffer.
                while let Some(header) = MessageHeader::try_read(&mut &self.buffer[.. self.length]) {

                    self.log_sender
                        .when_enabled(|l|
                            l.log(::logging::CommsEvent::Communication(
                                ::logging::CommunicationEvent {
                                    is_send: false,
                                    comm_channel: header.channel,
                                    source: header.source,
                                    target: header.target,
                                    seqno: header.seqno,
                                })
                            )
                        );

                    // TODO: Consolidate message sequences sent to the same worker.
                    let bytes = self.buffer.extract_to(header.required_bytes());
                    self.targets[header.target - self.worker_offset].push(bytes);
                }
            }
        }
    }

    // structure in charge of sending data to a Writer, for example the network
    struct BinarySender<W: Write> {
        writer:     W,
        sources:    Vec<SharedQueue<Bytes>>,
        log_sender: ::logging::CommsLogger,
    }

    impl<W: Write> BinarySender<W> {
        fn new(writer: W, sources: Vec<SharedQueue<Bytes>>, log_sender: ::logging::CommsLogger) -> BinarySender<W> {
            BinarySender { writer, sources, log_sender }
        }

        fn send_loop(&mut self) {

            let mut stash = Vec::new();

            // TODO: The previous TCP code exited cleanly when all inputs were dropped.
            //       Here we don't have inputs that drop, but perhaps we can instead
            //       notice when the other side has hung up (the `Arc` in `SharedQueue`).

            while !self.sources.is_empty() {

                for source in self.sources.iter_mut() {
                    while let Some(bytes) = source.pop() {
                        stash.push(bytes);
                    }
                }

                // If we got zero data, check that everyone is still alive.
                if stash.is_empty() {
                    self.sources.retain(|x| !x.is_done());
                }

                for bytes in stash.drain(..) {
                    self.writer.write_all(&bytes[..]);
                }

                self.writer.flush().unwrap();    // <-- because writer is buffered
            }
        }
    }
}

/// Builds an instance of a ProcessBinary.
///
/// Builders are required because some of the state in a `ProcessBinary` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct ProcessBinaryBuilder<BE: BytesExchange> {
    inner:      Process,
    index:      usize,          // number out of peers
    peers:      usize,          // number of peer allocators (for typed channel allocation).
    sends:      Vec<BE::Send>,  // for pushing bytes at each other process.
    recvs:      Vec<BE::Recv>,  // for pulling bytes from each other process.
}

impl<BE: BytesExchange> ProcessBinaryBuilder<BE> {
    /// Creates a vector of builders, sharing appropriate state.
    ///
    /// This method requires access to a byte exchanger, from which it mints channels.
    pub fn new_vector(
        mut byte_exchanger: BE,
        my_process: usize,
        threads: usize,
        processes: usize) -> Vec<ProcessBinaryBuilder<BE>> {

        Process::new_vector(threads)
            .into_iter()
            .enumerate()
            .map(|(index, inner)| {
                let (sends, recvs) = byte_exchanger.new(my_process * threads + index);
                ProcessBinaryBuilder {
                    inner,
                    index: my_process * threads + index,
                    peers: threads * processes,
                    sends,
                    recvs,
                }})
            .collect()
    }

    /// Builds a `ProcessBinary`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> ProcessBinary<BE> {

        let mut sends = Vec::new();
        for send in self.sends.into_iter() {
            sends.push(Rc::new(RefCell::new(send)));
        }

        ProcessBinary {
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
pub struct ProcessBinary<BE: BytesExchange> {

    inner:      Process,                            // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    allocated:  usize,                              // indicates how many channels have been allocated (locally).

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<BE::Send>>>,         // sends[x] -> goes to process x.
    recvs:      Vec<BE::Recv>,                      // recvs[x] <- from process x?.
    to_local:   Vec<Rc<RefCell<VecDeque<Bytes>>>>,  // to worker-local typed pullers.
}

impl<BE: BytesExchange> Allocate for ProcessBinary<BE> {
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
            let process_id = target_index / inner_peers;

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
                pushes.push(Box::new(Pusher::new(header, self.sends[process_id].clone())));
            }
        }

        while self.to_local.len() <= channel_id {
            self.to_local.push(Rc::new(RefCell::new(VecDeque::new())));
        }

        let puller = Box::new(Puller::new(inner_recv, self.to_local[channel_id].clone()));

        (pushes, puller, None)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn pre_work(&mut self) {

        for recv in self.recvs.iter_mut() {

            while let Some(mut bytes) = recv.receive() {

                // TODO: We could wrap `bytes` in a bytes::rc::Bytes,
                //       which could reduce `Arc` overhead, if it hurts.
                //       This new `Arc` should be local/uncontended, though.
                let mut bytes = Bytes::from(bytes);

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

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
struct Pusher<T, S: SendEndpoint> {
    header:     MessageHeader,
    sender:     Rc<RefCell<S>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T, S:SendEndpoint> Pusher<T, S> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: MessageHeader, sender: Rc<RefCell<S>>) -> Pusher<T,S> {
        Pusher {
            header:     header,
            sender:     sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data, S:SendEndpoint> Push<Message<T>> for Pusher<T, S> {
    #[inline]
    fn push(&mut self, element: &mut Option<Message<T>>) {
        if let Some(ref mut element) = *element {

            // determine byte lengths and build header.
            let mut header = self.header;
            self.header.seqno += 1;
            header.length = element.length_in_bytes();

            // acquire byte buffer and write header, element.
            let mut borrow = self.sender.borrow_mut();
            let mut bytes = borrow.reserve(header.required_bytes());
            header.write_to(&mut bytes).expect("failed to write header!");

            element.into_bytes(&mut bytes);

        }
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
struct Puller<T> {
    inner: Box<Pull<Message<T>>>,            // inner pullable (e.g. intra-process typed queue)
    current: Option<Message<T>>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>,    // source of serialized buffers
}

impl<T:Data> Puller<T> {
    fn new(inner: Box<Pull<Message<T>>>, receiver: Rc<RefCell<VecDeque<Bytes>>>) -> Puller<T> {
        Puller {
            inner,
            current: None,
            receiver,
        }
    }
}

impl<T:Data> Pull<Message<T>> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<Message<T>> {

        let inner = self.inner.pull();
        if inner.is_some() {
            inner
        }
        else {
            self.current =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(|bytes| unsafe { Message::from_bytes(bytes) });

            &mut self.current
        }
    }
}