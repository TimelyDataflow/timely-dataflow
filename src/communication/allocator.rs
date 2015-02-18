use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use communication::{Observer, Pushable, Pullable};
use networking::networking::MessageHeader;

// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.
// pub trait Communicator : 'static {
//     fn index(&self) -> u64;     // number out of peers
//     fn peers(&self) -> u64;     // number of peers
//     fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>);
// }

pub enum Communicator {
    Thread(Box<ThreadCommunicator>),     // same thread communicator (mostly no-ops)
    Process(Box<ProcessCommunicator>),   // same process communicator (typed channels)
    Binary(Box<BinaryCommunicator>),     // remote communicators (serialized channels)
}

impl Communicator {
    pub fn index(&self) -> u64 {
        match self {
            &Communicator::Thread(ref t)  => t.index(),
            &Communicator::Process(ref p) => p.index(),
            &Communicator::Binary(ref b)  => b.index(),
        }
    }
    pub fn peers(&self) -> u64 {
        match self {
            &Communicator::Thread(ref t)  => t.peers(),
            &Communicator::Process(ref p) => p.peers(),
            &Communicator::Binary(ref b)  => b.peers(),
        }
    }
    pub fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        match self {
            &mut Communicator::Thread(ref mut t)  => t.new_channel(),
            &mut Communicator::Process(ref mut p) => p.new_channel(),
            &mut Communicator::Binary(ref mut b)  => b.new_channel(),
        }
    }
}

// The simplest communicator remains worker-local and just queues sent messages.
pub struct ThreadCommunicator;
impl ThreadCommunicator {
    fn index(&self) -> u64 { 0 }
    fn peers(&self) -> u64 { 1 }
    // fn inner<'a>(&'a mut self) -> &'a mut Communicator { self }
    fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        return (vec![Box::new(shared.clone()) as Box<Pushable<T>>], Box::new(shared.clone()) as Box<Pullable<T>>)
    }
}


// A specific Communicator for inter-thread intra-process communication
pub struct ProcessCommunicator {
    inner:      Communicator,                   // inner ThreadCommunicator
    index:      u64,                            // number out of peers
    peers:      u64,                            // number of peer allocators (for typed channel allocation).
    allocated:  u64,                            // indicates how many have been allocated (locally).
    channels:   Arc<Mutex<Vec<Box<Any+Send>>>>, // Box<Any+Send> -> Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>
}

impl ProcessCommunicator {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
    fn inner<'a>(&'a mut self) -> &'a Communicator { &mut self.inner }
    fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let mut channels = self.channels.lock().ok().expect("mutex error?");
        if self.allocated == channels.len() as u64 {  // we need a new channel ...
            let mut senders = Vec::new();
            let mut receivers = Vec::new();
            for _ in range(0, self.peers) {
                let (s, r): (Sender<T>, Receiver<T>) = channel();
                senders.push(s);
                receivers.push(r);
            }

            let mut to_box = Vec::new();
            for recv in receivers.drain() {
                to_box.push(Some((senders.clone(), recv)));
            }

            channels.push(Box::new(to_box));
        }

        match channels[self.allocated as usize].downcast_mut::<(Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>)>() {
            Some(ref mut vector) => {
                self.allocated += 1;
                let (mut send, recv) = vector[self.index as usize].take().unwrap();
                let mut temp = Vec::new();
                for s in send.drain() { temp.push(Box::new(s) as Box<Pushable<T>>); }
                return (temp, Box::new(recv) as Box<Pullable<T>>)
            }
            _ => { panic!("unable to cast channel correctly"); }
        }
    }

    pub fn new_vector(count: u64) -> Vec<Communicator> {
        let channels = Arc::new(Mutex::new(Vec::new()));
        return (0 .. count).map(|index| Communicator::Process(Box::new(ProcessCommunicator {
            inner: Communicator::Thread(Box::new(ThreadCommunicator)),
            index: index,
            peers: count,
            allocated: 0,
            channels: channels.clone(),
        }))).collect();
    }
}


// A communicator intended for binary channels (networking, pipes, shared memory)
pub struct BinaryCommunicator {
    pub inner:          Communicator,    // inner ProcessCommunicator (use for process-local channels)
    pub index:          u64,             // index of this worker
    pub peers:          u64,             // number of peer workers
    pub graph:          u64,             // identifier for the current graph
    pub allocated:      u64,             // indicates how many channels have been allocated (locally).

    // for loading up state in the networking threads.
    pub writers:        Vec<Sender<((u64, u64, u64), Sender<Vec<u8>>)>>,                     // (index, back-to-worker)
    pub readers:        Vec<Sender<((u64, u64, u64), Sender<Vec<u8>>, Receiver<Vec<u8>>)>>,  // (index, data-to-worker, back-from-worker)

    pub writer_senders: Vec<Sender<(MessageHeader, Vec<u8>)>>
}

// A Communicator backed by Sender<Vec<u8>>/Receiver<Vec<u8>> pairs (e.g. networking, shared memory, files, pipes)

// We would like new_channel to allocate binary pushables and pullables for remote workers,
// and pass through to a ProcessCommunicator for process-local workers.

impl BinaryCommunicator {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
    fn inner<'a>(&'a mut self) -> &'a Communicator { &mut self.inner }
    fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {

        // built-up vector of Box<Pushable<T>> to return
        let mut pushers: Vec<Box<Pushable<T>>> = Vec::new();

        // we'll need process-local channels as well (no self-loop binary connection in this design; perhaps should allow)
        let inner_peers = self.inner.peers();
        let (mut inner_sends, inner_recv) = self.inner.new_channel();

        for index in (0..self.peers) {
            // for each peer, we need a Box<Pushable<T>>
            if index / inner_peers == self.index / inner_peers {
                // if this is process-local use the process-local pushable
                pushers.push(inner_sends.remove(0));
            }
            else {
                // otherwise, we'll need to prep a BinaryPushable<T>

                // generate a binary (Vec<u8>) channel pair of (back_to_worker, back_from_net)
                let (s,r) = channel();

                let mut sender_idx = index / inner_peers;
                if sender_idx > self.index / inner_peers {
                    sender_idx -= 1;
                }

                pushers.push(Box::new(BinaryPushable {
                    sender:     self.writer_senders[sender_idx as usize].clone(),    // where to send serialized messages
                    receiver:   r,                                          // what to tug to get empty vectors
                    buffer:     Vec::new(),                                 // buffer of deserialized (T) records
                    threshold:  256,                                        // how many to deserialize before breaking (I think)
                }));

                self.writers[sender_idx as usize].send(((self.index, self.graph, self.allocated), s)).ok().expect("send error");
            }
        }

        let (send,recv) = channel();    // binary channel from binary listener to BinaryPullable<T>

        let mut pullsends = Vec::new();
        for reader in self.readers.iter() {
            let (s,r) = channel();
            pullsends.push(s);

            // register interest in hearing about data for self.index using the binary channel
            reader.send(((self.index, self.graph, self.allocated), send.clone(), r)).ok().expect("send error");
        }

        let pullable = Box::new(BinaryPullable {
            inner:      inner_recv,
            senders:    pullsends,
            receiver:   recv,
            staged:     Vec::new(),
            cursor:     0,
        });

        return (pushers, pullable);
    }
}

struct BinaryPushable<T> {
    sender:     Sender<(MessageHeader, Vec<u8>)>,   // targets for each remote destination
    receiver:   Receiver<Vec<u8>>,                  // source of empty binary vectors
    buffer:     Vec<T>,                             // typed buffers (pre-serialization)
    threshold:  usize,
}

impl<T:'static> Pushable<T> for BinaryPushable<T> {
    #[inline]
    fn push(&mut self, data: T) {
        self.buffer.push(data);
        if self.buffer.len() > self.threshold {
            assert!(false);
            // TODO : serialize that stuff and send it
            // TODO : ...
        }
    }
}

struct BinaryPullable<T> {
    inner:      Box<Pullable<T>>,       // inner pullable (e.g. intra-process typed queue)
    senders:    Vec<Sender<Vec<u8>>>,   // places to put used binary vectors
    receiver:   Receiver<Vec<u8>>,      // source of serialized buffers
    staged:     Vec<T>,                 // deserialized data, in progress.
    cursor:     usize,                  // cursor into self.staged
}

impl<T:'static> Pullable<T> for BinaryPullable<T> {
    #[inline]
    fn pull(&mut self) -> Option<T> {
        if let Some(data) = self.inner.pull() {
            Some(data)
        }
        else {
            if self.staged.len() == 0 {
                if let Some(serialized) = self.receiver.try_recv().ok() {
                    assert!(serialized.len() > 0);
                    assert!(false)
                    // TODO : deserialize stuff in to self.staged
                    // TODO : ...
                }
            }

            self.staged.pop()   // TODO : worry about the fact that this is probably in the wrong order
        }
    }
}
