use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;
use std::marker::PhantomData;

use columnar::{Columnar, ColumnarStack};
use communication::{Pushable, Pullable};
use networking::networking::MessageHeader;
use std::default::Default;

// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.
pub trait Communicator: 'static {
    fn index(&self) -> u64;     // number out of peers
    fn peers(&self) -> u64;     // number of peers
    fn new_channel<T:Send+Columnar+Any>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>);
}

// TODO : Would be nice if Communicator had associated types for its Pushable and Pullable types,
// TODO : but they would have to be generic over T, with the current set-up. Might require HKT?

// impl<'a, C: Communicator + 'a> Communicator for &'a mut C {
//     fn index(&self) -> u64 { (**self).index() }
//     fn peers(&self) -> u64 { (**self).peers() }
//     fn new_channel<T:Send+Columnar+Any>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) { (**self).new_channel() }
// }

// The simplest communicator remains worker-local and just queues sent messages.
pub struct ThreadCommunicator;
impl Communicator for ThreadCommunicator {
    fn index(&self) -> u64 { 0 }
    fn peers(&self) -> u64 { 1 }
    fn new_channel<T:'static>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let shared = Rc::new(RefCell::new(VecDeque::<T>::new()));
        return (vec![Box::new(shared.clone()) as Box<Pushable<T>>], Box::new(shared.clone()) as Box<Pullable<T>>)
    }
}


// A specific Communicator for inter-thread intra-process communication
pub struct ProcessCommunicator {
    inner:      ThreadCommunicator,             // inner ThreadCommunicator
    index:      u64,                            // number out of peers
    peers:      u64,                            // number of peer allocators (for typed channel allocation).
    allocated:  u64,                            // indicates how many have been allocated (locally).
    channels:   Arc<Mutex<Vec<Box<Any+Send>>>>, // Box<Any+Send> -> Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>
}

impl ProcessCommunicator {
    pub fn inner<'a>(&'a mut self) -> &'a mut ThreadCommunicator { &mut self.inner }
    pub fn new_vector(count: u64) -> Vec<ProcessCommunicator> {
        let channels = Arc::new(Mutex::new(Vec::new()));
        return (0 .. count).map(|index| ProcessCommunicator {
            inner:      ThreadCommunicator,
            index:      index,
            peers:      count,
            allocated:  0,
            channels:   channels.clone(),
        }).collect();
    }
}

impl Communicator for ProcessCommunicator {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
    fn new_channel<T:Send+Any>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let mut channels = self.channels.lock().ok().expect("mutex error?");
        if self.allocated == channels.len() as u64 {  // we need a new channel ...
            let mut senders = Vec::new();
            let mut receivers = Vec::new();
            for _ in (0..self.peers) {
                let (s, r): (Sender<T>, Receiver<T>) = channel();
                senders.push(s);
                receivers.push(r);
            }

            let mut to_box = Vec::new();
            for recv in receivers.drain(..) {
                to_box.push(Some((senders.clone(), recv)));
            }

            channels.push(Box::new(to_box));
        }

        match channels[self.allocated as usize].downcast_mut::<(Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>)>() {
            Some(ref mut vector) => {
                self.allocated += 1;
                let (mut send, recv) = vector[self.index as usize].take().unwrap();
                let mut temp = Vec::new();
                for s in send.drain(..) { temp.push(Box::new(s) as Box<Pushable<T>>); }
                return (temp, Box::new(recv) as Box<Pullable<T>>)
            }
            _ => { panic!("unable to cast channel correctly"); }
        }
    }
}


// A communicator intended for binary channels (networking, pipes, shared memory)
pub struct BinaryCommunicator {
    pub inner:      ProcessCommunicator,    // inner ProcessCommunicator (use for process-local channels)
    pub index:      u64,                    // index of this worker
    pub peers:      u64,                    // number of peer workers
    pub graph:      u64,                    // identifier for the current graph
    pub allocated:  u64,                    // indicates how many channels have been allocated (locally).

    // for loading up state in the networking threads.
    pub writers:    Vec<Sender<((u64, u64, u64), Sender<Vec<u8>>)>>,                     // (index, back-to-worker)
    pub readers:    Vec<Sender<((u64, u64, u64), Sender<Vec<u8>>, Receiver<Vec<u8>>)>>,  // (index, data-to-worker, back-from-worker)
    pub senders:    Vec<Sender<(MessageHeader, Vec<u8>)>>                                // for sending bytes!
}

impl BinaryCommunicator {
    pub fn inner<'a>(&'a mut self) -> &'a mut ProcessCommunicator { &mut self.inner }
}

// A Communicator backed by Sender<Vec<u8>>/Receiver<Vec<u8>> pairs (e.g. networking, shared memory, files, pipes)
impl Communicator for BinaryCommunicator {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
    fn new_channel<T:Send+Columnar+Any>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let mut pushers: Vec<Box<Pushable<T>>> = Vec::new(); // built-up vector of Box<Pushable<T>> to return

        // we'll need process-local channels as well (no self-loop binary connection in this design; perhaps should allow)
        let inner_peers = self.inner.peers();
        let (inner_sends, inner_recv) = self.inner.new_channel();

        // prep a pushable for each endpoint, multiplied by inner_peers
        for (index, writer) in self.writers.iter().enumerate() {
            for _ in (0..inner_peers) {
                let (s,r) = channel();  // generate a binary (Vec<u8>) channel pair of (back_to_worker, back_from_net)
                let target_index = if index as u64 >= (self.index * inner_peers) { index as u64 + inner_peers } else { index as u64 };
                println!("init'ing send channel: ({} {} {})", self.index, self.graph, self.allocated);
                writer.send(((self.index, self.graph, self.allocated), s)).unwrap();
                let header = MessageHeader {
                    graph:      self.graph,
                    channel:    self.allocated,
                    source:     self.index,
                    target:     target_index,
                    length:     0,
                };
                pushers.push(Box::new(BinaryPushable::new(header, self.senders[index].clone(), r)));
            }
        }

        // splice inner_sends into the vector of pushables
        for (index, writer) in inner_sends.into_iter().enumerate() {
            pushers.insert((self.index * inner_peers) as usize + index, writer);
        }

        // prep a Box<Pullable<T>> using inner_recv and fresh registered pullables
        let (send,recv) = channel();    // binary channel from binary listener to BinaryPullable<T>
        let mut pullsends = Vec::new();
        for reader in self.readers.iter() {
            let (s,r) = channel();
            pullsends.push(s);
            println!("init'ing recv channel: ({} {} {})", self.index, self.graph, self.allocated);
            reader.send(((self.index, self.graph, self.allocated), send.clone(), r)).unwrap();
        }

        let pullable = Box::new(BinaryPullable {
            inner:      inner_recv,
            senders:    pullsends,
            receiver:   recv,
            stack:      Default::default(),
        });

        self.allocated += 1;

        return (pushers, pullable);
    }
}

struct BinaryPushable<T: Columnar> {
    header:     MessageHeader,
    sender:     Sender<(MessageHeader, Vec<u8>)>,   // targets for each remote destination
    receiver:   Receiver<Vec<u8>>,                  // source of empty binary vectors
    phantom:    PhantomData<T>,
    buffer:     Vec<u8>,
    stack:      <T as Columnar>::Stack,
}

impl<T: Columnar> BinaryPushable<T> {
    pub fn new(header: MessageHeader, sender: Sender<(MessageHeader, Vec<u8>)>, receiver: Receiver<Vec<u8>>) -> BinaryPushable<T> {
        BinaryPushable {
            header:     header,
            sender:     sender,
            receiver:   receiver,
            phantom:    PhantomData,
            buffer:     Vec::new(),
            stack:      Default::default(),
        }
    }
}

impl<T:Columnar+'static> Pushable<T> for BinaryPushable<T> {
    #[inline]
    fn push(&mut self, data: T) {
        let mut bytes = if let Some(buffer) = self.receiver.try_recv().ok() { buffer } else { Vec::new() };
        bytes.clear();

        self.stack.push(data);
        self.stack.encode(&mut bytes).unwrap();

        let mut header = self.header;
        header.length = bytes.len() as u64;

        self.sender.send((header, bytes)).ok();
    }
}

struct BinaryPullable<T: Columnar> {
    inner:      Box<Pullable<T>>,       // inner pullable (e.g. intra-process typed queue)
    senders:    Vec<Sender<Vec<u8>>>,   // places to put used binary vectors
    receiver:   Receiver<Vec<u8>>,      // source of serialized buffers
    stack:      <T as Columnar>::Stack,
}

impl<T:Columnar+'static> Pullable<T> for BinaryPullable<T> {
    #[inline]
    fn pull(&mut self) -> Option<T> {
        if let Some(data) = self.inner.pull() { Some(data) }
        else if let Some(bytes) = self.receiver.try_recv().ok() {
            self.stack.decode(&mut &bytes[..]).unwrap();
            self.senders[0].send(bytes).unwrap();                   // TODO : Not clear where bytes came from; find out!
            self.stack.pop()
        }
        else { None }
    }
}
