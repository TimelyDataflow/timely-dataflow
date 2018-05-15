
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::DerefMut;

use bytes::arc::Bytes;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull, Serialize};

pub trait BytesExchange {
    type Send: SendEndpoint+'static;
    type Recv: RecvEndpoint+'static;
    fn new() -> (Self::Send, Self::Recv);
}

pub trait SendEndpoint {
    type SendBuffer: ::std::io::Write;
    fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer;
    fn publish(&mut self);
}

pub trait RecvEndpoint {
    type RecvBuffer: DerefMut<Target=[u8]>;
    fn receive(&mut self) -> Option<Self::RecvBuffer>;
    fn recycle(&mut self, buffer: Self::RecvBuffer);
}

pub mod vec {

    use std::sync::mpsc::{Sender, Receiver, channel};

    use super::{BytesExchange, SendEndpoint, RecvEndpoint};

    pub struct VecSendEndpoint {
        send: Sender<Vec<u8>>,      // send full vectors
        recv: Receiver<Vec<u8>>,    // recv empty vectors
        balance: usize,             // #sent - #recv.

        buffer: Vec<u8>,
        stash:  Vec<Vec<u8>>,       // spare buffers
        default_size: usize,
    }

    impl VecSendEndpoint {
        /// Drains `self.recv` of empty buffers, stashes them.
        fn drain_recv(&mut self) {
            while let Ok(bytes) = self.recv.try_recv() {
                self.balance -= 1;
                if bytes.capacity() == self.default_size {
                    self.stash.push(bytes);
                }
            }
        }
        /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
        fn send_buffer(&mut self) {

            // println!("sending buffer of {:?} bytes", self.buffer.len());

            let buffer = ::std::mem::replace(&mut self.buffer, Vec::new());
            if buffer.len() > 0 {
                self.send.send(buffer).expect("VecSendEndpoint::send_buffer(): failed to send buffer");
                self.balance += 1;
            }
            else {
                if buffer.capacity() == self.default_size {
                    self.stash.push(buffer);
                }
            }
        }
    }

    impl SendEndpoint for VecSendEndpoint {

        type SendBuffer = Vec<u8>;

        fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer {

            // println!("reserving {:?} bytes", capacity);

            if self.buffer.capacity() < capacity + self.buffer.len() {
                self.send_buffer();
                if capacity > self.default_size {
                    self.buffer = Vec::with_capacity(capacity);
                }
                else {
                    self.drain_recv();
                    self.buffer = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(self.default_size))
                }
            }

            &mut self.buffer
        }

        fn publish(&mut self) {
            self.drain_recv();
            if self.balance == 0 {
                self.send_buffer();
            }
        }
    }

    pub struct VecRecvEndpoint {
        recv: Receiver<Vec<u8>>,    // recv full vectors
        send: Sender<Vec<u8>>,      // send empty vectors
    }

    impl RecvEndpoint for VecRecvEndpoint {
        type RecvBuffer = Vec<u8>;
        fn receive(&mut self) -> Option<Self::RecvBuffer> {
            if let Ok(bytes) = self.recv.try_recv() {
                Some(bytes)
            }
            else { None }
        }
        fn recycle(&mut self, mut buffer: Self::RecvBuffer) {
            buffer.clear();
            // other side hanging up shouldn't cause panic.
            let _ = self.send.send(buffer);
        }
    }

    pub struct VecBytesExchange;

    impl BytesExchange for VecBytesExchange {
        type Send = VecSendEndpoint;
        type Recv = VecRecvEndpoint;
        fn new() -> (Self::Send, Self::Recv) {

            let (send1, recv1) = channel();
            let (send2, recv2) = channel();

            let result1 = VecSendEndpoint {
                send: send1,
                recv: recv2,
                balance: 0,
                buffer: Vec::new(),
                stash: Vec::new(),
                default_size: 1 << 20,
            };
            let result2 = VecRecvEndpoint {
                send: send2,
                recv: recv1,
            };

            (result1, result2)
        }
    }
}


pub struct ProcessBinaryBuilder<BE: BytesExchange> {
    index:      usize,  // number out of peers
    peers:      usize,  // number of peer allocators (for typed channel allocation).
    sends:      Vec<BE::Send>, // with each other worker (for pushing bytes)
    recvs:      Vec<BE::Recv>, // with each other worker (for pulling bytes)
}

impl<BE: BytesExchange> ProcessBinaryBuilder<BE> {

    pub fn new_vector(count: usize) -> Vec<ProcessBinaryBuilder<BE>> {

        let mut sends = Vec::new();
        let mut recvs = Vec::new();
        for _ in 0 .. count { sends.push(Vec::new()); }
        for _ in 0 .. count { recvs.push(Vec::new()); }

        for source in 0 .. count {
            for target in 0 .. count {
                let (send, recv) = BE::new();
                sends[source].push(send);
                recvs[target].push(recv);
            }
        }

        let mut result = Vec::new();
        for (index, (sends, recvs)) in sends.drain(..).zip(recvs.drain(..)).enumerate() {
            result.push(ProcessBinaryBuilder {
                index,
                peers: count,
                sends,
                recvs,
            })
        }

        result
    }

    pub fn build(self) -> ProcessBinary<BE> {
        let mut shared = Vec::new();
        for send in self.sends.into_iter() {
            shared.push(Rc::new(RefCell::new(send)));
        }

        ProcessBinary {
            index: self.index,
            peers: self.peers,
            allocated: 0,
            sends: shared,
            recvs: self.recvs,
            to_local: Vec::new(),
            in_progress: Vec::new(),
        }
    }
}

// A specific Communicator for inter-thread intra-process communication
pub struct ProcessBinary<BE: BytesExchange> {
    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    allocated:  usize,                              // indicates how many have been allocated (locally).
    sends:      Vec<Rc<RefCell<BE::Send>>>,         // channels[x] -> goes to worker x.
    recvs:      Vec<BE::Recv>,                      // from all other workers.
    to_local:   Vec<Rc<RefCell<VecDeque<Bytes>>>>,  // to worker-local typed pullers.

    in_progress: Vec<Option<(Bytes, usize)>>,
}

impl<BE: BytesExchange> Allocate for ProcessBinary<BE> {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) {

        let channel_id = self.allocated;
        self.allocated += 1;

        let mut pushes = Vec::<Box<Push<T>>>::new();

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

        let puller = Box::new(Puller::new(self.to_local[channel_id].clone(), channel_id));

        (pushes, puller, None)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn pre_work(&mut self) {

        let recvs_len = self.recvs.len();
        for (peer, recv) in self.recvs.iter_mut().enumerate() {

            while let Some(mut bytes) = recv.receive() {

                let mut bytes = Bytes::from(bytes);

                // we are guaranteed that `bytes` contains exactly an integral number of messages.
                // no splitting occurs across allocations.
                while bytes.len() > 0 {

                    if let Some(header) = MessageHeader::try_read(&mut &bytes[..]) {

                        // Get the header and payload, ditch the header.
                        let mut peel = bytes.extract_to(header.required_bytes());
                        peel.extract_to(40);

                        while self.to_local.len() <= header.channel {
                            self.to_local.push(Rc::new(RefCell::new(VecDeque::new())));
                        }

                        self.to_local[header.channel].borrow_mut().push_back(peel);
                    }
                    else {
                        println!("failed to read full header!");
                    }
                }

                assert!(peer < recvs_len);
                self.in_progress.push(Some((bytes, peer)));
            }
        }
    }

    // Perform postparatory work, most likely sending incomplete binary buffers.
    fn post_work(&mut self) {

        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        for index in 0 .. self.to_local.len() {
            let len = self.to_local[index].borrow_mut().len();
            if len > 0 {
                println!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
            }
        }

        // Recycle completely processed byte buffers.
        for index in 0 .. self.in_progress.len() {
            if let Some((bytes, peer)) = self.in_progress[index].take() {
                match bytes.try_recover::<<BE::Recv as RecvEndpoint>::RecvBuffer>() {
                    Ok(vec) => {
                        self.recvs[peer].recycle(vec);
                    }
                    Err(bytes) => {
                        self.in_progress[index] = Some((bytes, peer));
                    }
                }
            }
        }
        self.in_progress.retain(|x| x.is_some());
    }
}

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
struct Pusher<T,S:SendEndpoint> {
    header:     MessageHeader,
    sender:     Rc<RefCell<S>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T,S:SendEndpoint> Pusher<T,S> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: MessageHeader, sender: Rc<RefCell<S>>) -> Pusher<T,S> {
        Pusher {
            header:     header,
            sender:     sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data,S:SendEndpoint> Push<T> for Pusher<T,S> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        if let Some(ref mut element) = *element {

            // determine byte lengths and build header.
            let element_length = element.length_in_bytes();
            let mut header = self.header;
            self.header.seqno += 1;
            header.length = element_length;

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
    channel: usize,
    current: Option<T>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>,    // source of serialized buffers
}
impl<T:Data> Puller<T> {
    fn new(receiver: Rc<RefCell<VecDeque<Bytes>>>, channel: usize) -> Puller<T> {
        Puller { channel, current: None, receiver }
    }
}

impl<T:Data> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {

        self.current =
        self.receiver
            .borrow_mut()
            .pop_front()
            .map(|bytes| <T as Serialize>::from_bytes(bytes));

        &mut self.current
    }
}