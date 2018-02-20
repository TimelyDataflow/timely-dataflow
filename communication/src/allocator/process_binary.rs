
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;

use networking::MessageHeader;

use {Allocate, Data, Push, Pull, Serialize};
// use allocator::{Allocate, Thread};
// use {Push, Pull};

pub struct ProcessBinaryBuilder {
    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    recv:       Receiver<Vec<u8>>,                  // from all other workers.
    sends:      Vec<Sender<Vec<u8>>>,               // to each other worker.
}

impl ProcessBinaryBuilder {

    pub fn new_vector(count: usize) -> Vec<ProcessBinaryBuilder> {

        let mut sends = Vec::new();
        let mut recvs = Vec::new();

        for _index in 0 .. count {
            let (send, recv) = channel();
            sends.push(send);
            recvs.push(recv);
        }

        let mut result = Vec::new();
        for (index, recv) in recvs.drain(..).enumerate() {
            result.push(ProcessBinaryBuilder {
                index,
                peers: count,
                sends: sends.clone(),
                recv,
            });
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
            recv: self.recv,
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
    recv:       Receiver<Vec<u8>>,                  // from all other workers.
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
    fn pre_work(&mut self) {

        while let Ok(bytes) = self.recv.try_recv() {

            // we are guaranteed that `bytes` contains exactly an integral number of messages.
            // no splitting occurs across allocations.

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
    }

    // Perform postparatory work, most likely sending incomplete binary buffers.
    fn post_work(&mut self) {
        for send in self.sends.iter_mut() {
            send.borrow_mut().flush();
        }
    }
}

struct SharedByteBuffer {
    sender:     Sender<Vec<u8>>,    // channels for each destination worker.
    buffer:     Vec<u8>,            // working space for each destination worker.
    length:     usize,              // currently occupied prefix bytes.
    capacity:   usize,              // default capacity.
}

impl SharedByteBuffer {

    // Allocates a new SharedByteBuffer with an indicated default capacity.
    fn new(sender: Sender<Vec<u8>>) -> Self {
        SharedByteBuffer {
            sender,
            buffer: vec![0; 1 << 10],
            length: 0,
            capacity: 1 << 10,
        }
    }

    // Retrieve a writeable buffer of `size` bytes.
    //
    // This may result in the current working buffer being sent and a new buffer
    // being acquired or allocated.
    fn reserve(&mut self, size: usize) -> &mut [u8] {

        if self.length + size > self.buffer.len() {

            // if we have filled our current buffer, double the capacity of the next buffer.
            // current rule of thumb: double up to 1 << 20 bytes, stop doubling then.
            if self.capacity < (1 << 20) {
                self.capacity *= 2;
            }

            let capacity = ::std::cmp::max(self.capacity, size.next_power_of_two());
            let new_buffer = vec![0; capacity];
            let mut old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);
            old_buffer.truncate(self.length);
            if old_buffer.len() > 0 {
                self.sender.send(old_buffer).expect("Reserve: failed to send.");
            }
            self.length = 0;
        }

        let offset = self.length;
        self.length += size;
        &mut self.buffer[offset..][..size]
    }

    // Push out all pending byte buffer sends.
    fn flush(&mut self) {
        if self.length > 0 {
            // reduce the capacity by a factor of two, for kicks.
            if self.capacity > 1 { self.capacity /= 2; }

            let new_buffer = vec![0; self.capacity];
            let mut old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);
            old_buffer.truncate(self.length);
            if old_buffer.len() > 0 {
                self.sender.send(old_buffer).expect("Flush: failed to send.");
            }
            self.length = 0;
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
            assert_eq!(bytes.len(), header.required_bytes());
            // println!("allocated {} bytes for {}", bytes.len(), header.required_bytes());
            header.write_to(&mut bytes).expect("failed to write header!");
            element.into_bytes(&mut bytes);
            assert_eq!(bytes.len(), 0);
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