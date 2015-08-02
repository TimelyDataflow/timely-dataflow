use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use drain::DrainExt;

use communication::{Communicator, Data, Message};
use communication::communicator::Thread;

// A specific Communicator for inter-thread intra-process communication
pub struct Process {
    inner:      Thread,             // inner Thread
    index:      usize,                            // number out of peers
    peers:      usize,                            // number of peer allocators (for typed channel allocation).
    allocated:  usize,                            // indicates how many have been allocated (locally).
    channels:   Arc<Mutex<Vec<Box<Any+Send>>>>, // Box<Any+Send> -> Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>
}

impl Process {
    pub fn inner<'a>(&'a mut self) -> &'a mut Thread { &mut self.inner }
    pub fn new_vector(count: usize) -> Vec<Process> {
        let channels = Arc::new(Mutex::new(Vec::new()));
        return (0 .. count).map(|index| Process {
            inner:      Thread,
            index:      index,
            peers:      count,
            allocated:  0,
            channels:   channels.clone(),
        }).collect();
    }
}

impl Communicator for Process {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn new_channel<T: Data, D: Data>(&mut self) -> (Vec<::communication::observer::BoxedObserver<T, D>>, Box<::communication::Pullable<T, D>>) {
        let mut channels = self.channels.lock().ok().expect("mutex error?");
        if self.allocated == channels.len() {  // we need a new channel ...
            let mut senders = Vec::new();
            let mut receivers = Vec::new();
            for _ in (0..self.peers) {
                let (s, r): (Sender<(T, Message<D>)>, Receiver<(T, Message<D>)>) = channel();
                senders.push(s);
                receivers.push(r);
            }

            let mut to_box = Vec::new();
            for recv in receivers.drain_temp() {
                to_box.push(Some((senders.clone(), recv)));
            }

            channels.push(Box::new(to_box));
        }

        match channels[self.allocated as usize].downcast_mut::<(Vec<Option<(Vec<Sender<(T, Message<D>)>>, Receiver<(T, Message<D>)>)>>)>() {
            Some(ref mut vector) => {
                self.allocated += 1;
                let (mut send, recv) = vector[self.index as usize].take().unwrap();
                let mut temp = Vec::new();
                for s in send.drain_temp() { temp.push(::communication::observer::BoxedObserver::new(Observer::new(s))); }
                return (temp, Box::new(Pullable::new(recv)) as Box<::communication::Pullable<T, D>>)
            }
            _ => { panic!("unable to cast channel correctly"); }
        }
    }
}

// an observer wrapping a Rust channel
struct Observer<T: Data, D: Data> {
    time: Option<T>,
    dest: Sender<(T, Message<D>)>,
    // shared: Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T: Data, D: Data> Observer<T, D> {
    fn new(dest: Sender<(T, Message<D>)>) -> Observer<T, D> {
        Observer {
            time: None,
            dest: dest,
        }
    }
}

impl<T: Data, D: Data> ::communication::observer::Observer for Observer<T, D> {
    type Time = T;
    type Data = D;
    #[inline] fn open(&mut self, time: &Self::Time) {
        assert!(self.time.is_none());
        self.time = Some(time.clone());
    }
    #[inline] fn shut(&mut self,_time: &Self::Time) {
        assert!(self.time.is_some());
        self.time = None;
    }
    #[inline] fn give(&mut self, data: &mut Message<Self::Data>) {
        assert!(self.time.is_some());
        if data.len() > 0 {
            if let Some(time) = self.time.clone() {
                // ALLOC : We replace with empty typed data. Not clear why typed is better than binary,
                // ALLOC : but lots of folks wouldn't expect bytes, and anyone should tolerate typed.
                self.dest.send((time, ::std::mem::replace(data, Message::from_typed(&mut Vec::new())))).unwrap();
            }
        }
    }
}

struct Pullable<T: Data, D: Data> {
    current: Option<(T, Message<D>)>,
    source: Receiver<(T, Message<D>)>,
    // shared: Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T: Data, D: Data> Pullable<T, D> {
    fn new(source: Receiver<(T, Message<D>)>) -> Pullable<T, D> {
        Pullable { current: None, source: source }
    }
}

impl<T: Data, D: Data> ::communication::pullable::Pullable<T, D> for Pullable<T, D> {
    #[inline]
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> {

        // TODO : here is where we would recycle data
        self.current = self.source.try_recv().ok();

        if let Some((_, ref data)) = self.current {
            assert!(data.len() > 0);
        }
        self.current.as_mut().map(|&mut (ref time, ref mut data)| (time, data))
    }
}
