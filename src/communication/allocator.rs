use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use communication::{Observer, Pushable, Pullable};

// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.

pub trait Communicator {
    fn index(&self) -> u64;     // number out of peers
    fn peers(&self) -> u64;     // number of peers
    fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>);
}

// The simplest communicator remains worker-local and just queues sent messages.

pub struct ThreadCommunicator;
impl Communicator for ThreadCommunicator {
    fn index(&self) -> u64 { 0 }
    fn peers(&self) -> u64 { 1 }
    fn new_channel<T:Send>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        return (vec![Box::new(shared.clone()) as Box<Pushable<T>>], Box::new(shared.clone()) as Box<Pullable<T>>)
    }
}

// A specific Communicator for inter-thread intra-process communication
pub struct ProcessCommunicator {
    index:      u64,                            // number out of peers
    peers:      u64,                            // number of peer allocators (for typed channel allocation).
    allocated:  u64,                            // indicates how many have been allocated (locally).
    channels:   Arc<Mutex<Vec<Box<Any+Send>>>>, // Box<Any+Send> -> Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>
}

impl Communicator for ProcessCommunicator {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
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
}

impl ProcessCommunicator {
    pub fn new_vector(count: u64) -> Vec<ProcessCommunicator> {
        let channels = Arc::new(Mutex::new(Vec::new()));
        return (0 .. count).map(|index| ProcessCommunicator {
            index: index,
            peers: count,
            allocated: 0,
            channels: channels.clone(),
        }).collect();
    }
}
