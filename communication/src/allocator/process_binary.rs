
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull, Serialize};

/// A unidirectional bulk byte channel
///
/// This channel means to accept large owned `Vec<u8>` buffers and hand them off to another
/// thread, who consumes them and returns emptied allocations. The intent here is that the
/// sender can notice when the channel is not empty and can consolidate writes to a large
/// buffer until that changes.
pub struct ByteExchange {
    send: Sender<Vec<u8>>,
    recv: Receiver<Vec<u8>>,
    balance: usize,             // number sent minus number received back.
}

impl ByteExchange {

    pub fn new_pair() -> (Self, Self) {

        let (send1, recv1) = channel();
        let (send2, recv2) = channel();

        let result1 = ByteExchange { send: send1, recv: recv2, balance: 0, };
        let result2 = ByteExchange { send: send2, recv: recv1, balance: 0, };

        (result1, result2)
    }

    #[inline(always)]
    pub fn queue_length(&self) -> usize {
        self.balance
    }
    pub fn send(&mut self, bytes: Vec<u8>) {
        // println!("sending: {:?}", bytes.len());
        // non-empty bytes are expected to return.
        if bytes.len() > 0 {
            // println!("incrementing balance: {:?}", self.balance);
            self.balance += 1;
        }
        self.send
            .send(bytes)
            .expect("failed to send!");
    }
    pub fn recv(&mut self) -> Option<Vec<u8>> {
        if let Ok(bytes) = self.recv.try_recv() {
            // println!("recving: {:?}", bytes.len());
            // empty bytes are returend buffers.
            if bytes.len() == 0 {
                assert!(self.balance > 0);
                // println!("decrementing balance: {:?}", self.balance);
                self.balance -= 1;
            }
            Some(bytes)
        }
        else {
            None
        }
    }
}

pub struct ProcessBinaryBuilder {
    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    sends:      Vec<ByteExchange>,                  // with each other worker (for pushing bytes)
    recvs:      Vec<ByteExchange>,                  // with each other worker (for pulling bytes)
}

impl ProcessBinaryBuilder {

    pub fn new_vector(count: usize) -> Vec<ProcessBinaryBuilder> {

        let mut sends = Vec::new();
        let mut recvs = Vec::new();
        for _ in 0 .. count { sends.push(Vec::new()); }
        for _ in 0 .. count { recvs.push(Vec::new()); }

        for source in 0 .. count {
            for target in 0 .. count {
                let (send, recv) = ByteExchange::new_pair();
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

    pub fn build(self) -> ProcessBinary {
        let mut shared = Vec::new();
        for send in self.sends.into_iter() {
            shared.push(Rc::new(RefCell::new(SharedByteBuffer::new(send))));
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
pub struct ProcessBinary {
    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    allocated:  usize,                              // indicates how many have been allocated (locally).
    sends:      Vec<Rc<RefCell<SharedByteBuffer>>>, // channels[x] -> goes to worker x.
    recvs:      Vec<ByteExchange>,                  // from all other workers.
    to_local:   Vec<Rc<RefCell<VecDeque<Vec<u8>>>>>,// to worker-local typed pullers.
}

impl Allocate for ProcessBinary {
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

            while let Some(mut bytes) = recv.recv() {

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

                bytes.clear();
                recv.send(bytes);
            }
        }
    }

    // Perform postparatory work, most likely sending incomplete binary buffers.
    fn post_work(&mut self) {
        for send in self.sends.iter_mut() {
            send.borrow_mut().flush();
        }
    }
}

struct SharedByteBuffer {
    sender: ByteExchange,    // channels for each destination worker.
    buffer: Vec<u8>,            // working space for each destination worker.
    stash:  Vec<Vec<u8>>,
}

impl SharedByteBuffer {

    // Allocates a new SharedByteBuffer with an indicated default capacity.
    fn new(sender: ByteExchange) -> Self {
        SharedByteBuffer {
            sender,
            buffer: Vec::with_capacity(1 << 20),
            stash: Vec::new(),
        }
    }

    // Retrieve a writeable buffer with at least `size` bytes available capacity.
    //
    // This may result in the current working buffer being sent and a new buffer
    // being acquired or allocated.
    fn reserve(&mut self, size: usize) -> &mut Vec<u8> {

        if self.buffer.len() + size > self.buffer.capacity() {

            // if we need more space than we expect from our byte exchange, ...
            let new_buffer = if size > (1 << 20) {
                Vec::with_capacity(size)
            }
            else {
                if self.stash.len() > 0 { self.stash.pop().unwrap() }
                else {
                    self.sender.recv().unwrap_or_else(|| { println!("allocating"); Vec::with_capacity(1 << 20) })
                }
            };

            assert!(new_buffer.is_empty());
            assert!(new_buffer.capacity() >= size);

            let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);
            if old_buffer.len() > 0 {
                self.sender.send(old_buffer);
            }
        }

        &mut self.buffer
    }

    // Push out all pending byte buffer sends.
    fn flush(&mut self) {
        if self.buffer.len() > 0 {

            while let Some(bytes) = self.sender.recv() {
                self.stash.push(bytes);
            }

            // only ship the buffer if the recipient has consumed everything we've sent them.
            // otherwise, wait for the first flush call when they have (they should eventually).
            if self.sender.queue_length() == 0 {
                let new_buffer = if self.stash.len() > 0 {
                    self.stash.pop().unwrap()
                }
                else {
                    self.sender.recv().unwrap_or_else(|| { println!("allocating"); Vec::with_capacity(1 << 20) })
                };
                let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);
                if old_buffer.len() > 0 {
                    self.sender.send(old_buffer);
                }
            }
            else {
                // println!("work to send, but queue length: {:?}", self.sender.queue_length());
            }
        }
    }
}


struct Pusher<T> {
    header:     MessageHeader,
    sender:     Rc<RefCell<SharedByteBuffer>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T> Pusher<T> {
    pub fn new(header: MessageHeader, sender: Rc<RefCell<SharedByteBuffer>>) -> Pusher<T> {
        Pusher {
            header:     header,
            sender:     sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data> Push<T> for Pusher<T> {
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

        self.current = self.receiver.borrow_mut().pop_front().map(|mut bytes| <T as Serialize>::from_bytes(&mut bytes));
        &mut self.current
    }
}