
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::DerefMut;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull, Serialize};

/// Reports the remaining capacity.
pub trait Available: ::std::io::Write {
    /// Reports the remaining capacity.
    ///
    /// The instance should be able to accept `self.available()` bytes written at it
    /// without complaint, and can have any (correct) behavior it likes on additional
    /// bytes written.
    fn available(&self) -> usize;
}

impl Available for Vec<u8> {
    fn available(&self) -> usize { self.capacity() - self.len() }
}

pub trait BytesExchange {
    type Send: BytesSendEndpoint+'static;
    type Recv: BytesRecvEndpoint+'static;
    fn new() -> (Self::Send, Self::Recv);
}

pub trait BytesSendEndpoint {
    type SendBuffer: Available;
    fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer;
    fn publish(&mut self);
}

pub trait BytesRecvEndpoint {
    type RecvBuffer: DerefMut<Target=[u8]>;
    fn receive(&mut self) -> Option<Self::RecvBuffer>;
    fn recycle(&mut self, buffer: Self::RecvBuffer);
}

pub mod vec {

    use std::sync::mpsc::{Sender, Receiver, channel};

    use super::{BytesExchange, BytesSendEndpoint, BytesRecvEndpoint};

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

    impl BytesSendEndpoint for VecSendEndpoint {

        type SendBuffer = Vec<u8>;

        fn reserve(&mut self, capacity: usize) -> &mut Self::SendBuffer {
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

    impl BytesRecvEndpoint for VecRecvEndpoint {
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
    to_local:   Vec<Rc<RefCell<VecDeque<Vec<u8>>>>>,// to worker-local typed pullers.
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

        let puller = Box::new(Puller::new(self.to_local[channel_id].clone()));

        (pushes, puller, None)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn pre_work(&mut self) {

        for recv in self.recvs.iter_mut() {

            while let Some(mut bytes) = recv.receive() {

                // we are guaranteed that `bytes` contains exactly an integral number of messages.
                // no splitting occurs across allocations.

                {
                    let mut slice = &bytes[..];
                    while let Some(header) = MessageHeader::try_read(&mut slice) {
                        let h_len = header.length as usize;  // length in bytes
                        let to_push = slice[..h_len].to_vec();
                        slice = &slice[h_len..];

                        while self.to_local.len() <= header.channel {
                            self.to_local.push(Rc::new(RefCell::new(VecDeque::new())));
                        }

                        self.to_local[header.channel].borrow_mut().push_back(to_push);
                    }
                    assert_eq!(slice.len(), 0);
                }

                recv.recycle(bytes);
            }
        }
    }

    // Perform postparatory work, most likely sending incomplete binary buffers.
    fn post_work(&mut self) {
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }
    }
}

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
struct Pusher<T,S:BytesSendEndpoint> {
    header:     MessageHeader,
    sender:     Rc<RefCell<S>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T,S:BytesSendEndpoint> Pusher<T,S> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: MessageHeader, sender: Rc<RefCell<S>>) -> Pusher<T,S> {
        Pusher {
            header:     header,
            sender:     sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data,S:BytesSendEndpoint> Push<T> for Pusher<T,S> {
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
    current: Option<T>,
    receiver: Rc<RefCell<VecDeque<Vec<u8>>>>,    // source of serialized buffers
}
impl<T:Data> Puller<T> {
    fn new(receiver: Rc<RefCell<VecDeque<Vec<u8>>>>) -> Puller<T> {
        Puller { current: None, receiver }
    }
}

impl<T:Data> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {

        self.current =
        self.receiver
            .borrow_mut()
            .pop_front()
            .map(|mut bytes| <T as Serialize>::from_bytes(&mut bytes));

        &mut self.current
    }
}