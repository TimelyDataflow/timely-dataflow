use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;
use std::marker::PhantomData;

use serialization::Serializable;
use communication::{Pushable, Pullable, Data};
use networking::networking::MessageHeader;
use drain::DrainExt;

// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.
pub trait Communicator: 'static {
    fn index(&self) -> u64;     // number out of peers
    fn peers(&self) -> u64;     // number of peers
    fn new_channel<T:Send+Serializable+Any+Data>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>);
}

// The simplest communicator remains worker-local and just queues sent messages.
pub struct ThreadCommunicator;
impl Communicator for ThreadCommunicator {
    fn index(&self) -> u64 { 0 }
    fn peers(&self) -> u64 { 1 }
    fn new_channel<T:'static>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let shared = Rc::new(RefCell::new(VecDeque::<T>::new()));
        return (vec![Box::new(shared.clone()) as Box<Pushable<T>>],
                Box::new(shared.clone()) as Box<Pullable<T>>)
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
    fn new_channel<T:Send+Any+Data>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
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
            for recv in receivers.drain_temp() {
                to_box.push(Some((senders.clone(), recv)));
            }

            channels.push(Box::new(to_box));
        }

        match channels[self.allocated as usize].downcast_mut::<(Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>)>() {
            Some(ref mut vector) => {
                self.allocated += 1;
                let (mut send, recv) = vector[self.index as usize].take().unwrap();
                let mut temp = Vec::new();
                for s in send.drain_temp() { temp.push(Box::new(s) as Box<Pushable<T>>); }
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
    pub writers:    Vec<Sender<((u64, u64, u64), Sender<Vec<u8>>)>>,
    pub readers:    Vec<Sender<((u64, u64, u64), (Sender<Vec<u8>>, Receiver<Vec<u8>>))>>,
    pub senders:    Vec<Sender<(MessageHeader, Vec<u8>)>>
}

impl BinaryCommunicator {
    pub fn inner<'a>(&'a mut self) -> &'a mut ProcessCommunicator { &mut self.inner }
}

// A Communicator backed by Sender<Vec<u8>>/Receiver<Vec<u8>> pairs (e.g. networking, shared memory, files, pipes)
impl Communicator for BinaryCommunicator {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
    fn new_channel<T:Send+Serializable+Any+Data>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let mut pushers: Vec<Box<Pushable<T>>> = Vec::new(); // built-up vector of Box<Pushable<T>> to return

        // we'll need process-local channels as well (no self-loop binary connection in this design; perhaps should allow)
        let inner_peers = self.inner.peers();
        let (inner_sends, inner_recv) = self.inner.new_channel();

        // prep a pushable for each endpoint, multiplied by inner_peers
        for (index, writer) in self.writers.iter().enumerate() {
            for counter in (0..inner_peers) {
                let (s,_r) = channel();  // TODO : Obviously this should be deleted...
                let mut target_index = index as u64 * inner_peers + counter as u64;

                // we may need to increment target_index by inner_peers;
                if index as u64 >= self.index / inner_peers { target_index += inner_peers; }

                writer.send(((self.index, self.graph, self.allocated), s)).unwrap();
                let header = MessageHeader {
                    graph:      self.graph,     // should be
                    channel:    self.allocated, //
                    source:     self.index,     //
                    target:     target_index,   //
                    length:     0,
                };
                pushers.push(Box::new(BinaryPushable::new(header, self.senders[index].clone())));
            }
        }

        // splice inner_sends into the vector of pushables
        for (index, writer) in inner_sends.into_iter().enumerate() {
            pushers.insert(((self.index / inner_peers) * inner_peers) as usize + index, writer);
        }

        // prep a Box<Pullable<T>> using inner_recv and fresh registered pullables
        let (send,recv) = channel();    // binary channel from binary listener to BinaryPullable<T>
        let mut pullsends = Vec::new();
        for reader in self.readers.iter() {
            let (s,r) = channel();
            pullsends.push(s);
            reader.send(((self.index, self.graph, self.allocated), (send.clone(), r))).unwrap();
        }

        let pullable = Box::new(BinaryPullable {
            inner:      inner_recv,
            receiver:   recv,
        });

        self.allocated += 1;

        return (pushers, pullable);
    }
}

struct BinaryPushable<T: Serializable> {
    header:     MessageHeader,
    sender:     Sender<(MessageHeader, Vec<u8>)>,   // targets for each remote destination
    phantom:    PhantomData<T>,
}

impl<T: Serializable> BinaryPushable<T> {
    pub fn new(header: MessageHeader, sender: Sender<(MessageHeader, Vec<u8>)>) -> BinaryPushable<T> {
        BinaryPushable {
            header:     header,
            sender:     sender,
            phantom:    PhantomData,
        }
    }
}

impl<T:Serializable+Data> Pushable<T> for BinaryPushable<T> {
    #[inline]
    fn push(&mut self, data: T) {
        let mut bytes = Vec::new();
        <T as Serializable>::encode(data, &mut bytes);

        let mut header = self.header;
        header.length = bytes.len() as u64;

        self.sender.send((header, bytes)).ok();
    }
}

struct BinaryPullable<T: Serializable> {
    inner:      Box<Pullable<T>>,       // inner pullable (e.g. intra-process typed queue)
    receiver:   Receiver<Vec<u8>>,      // source of serialized buffers
}

impl<T:Serializable+Data> Pullable<T> for BinaryPullable<T> {
    #[inline]
    fn pull(&mut self) -> Option<T> {
        if let Some(data) = self.inner.pull() { Some(data) }
        else if let Some(mut bytes) = self.receiver.try_recv().ok() {
            if let Ok(result) = <T as Serializable>::decode(&mut bytes) {
                Some (result)
            }
            else { None }
        }
        else { None }
    }
}
