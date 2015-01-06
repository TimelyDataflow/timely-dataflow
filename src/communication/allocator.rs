use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};


pub struct ChannelAllocator
{
    index:          uint,                           // number out of peers
    multiplicity:   uint,                           // number of peer allocators (for typed channel allocation).
    allocated:      uint,                           // indicates how many have been allocated (locally).
    channels:       Arc<Mutex<Vec<Box<Any+Send>>>>, // Box -> (Vec<Sender<type>>, Vec<Option<Receiver<type>>>).
}

impl ChannelAllocator
{
    pub fn index(&self) -> uint { self.index }
    pub fn multiplicity(&self) -> uint { self.multiplicity }

    pub fn new_channel<T:Send>(&mut self) -> (Vec<Sender<T>>, Option<Receiver<T>>)
    {
        let mut channels = self.channels.lock().ok().expect("mutex error?");

        // we need a new channel ...
        if self.allocated == channels.len()
        {
            let mut senders = Vec::new();
            let mut receivers = Vec::new();

            for _ in range(0, self.multiplicity)
            {
                let (s, r): (Sender<T>, Receiver<T>) = channel();
                senders.push(s);
                receivers.push(Some(r));
            }

            channels.push(box() (senders, receivers));
        }

        match channels[self.allocated].downcast_mut::<(Vec<Sender<T>>, Vec<Option<Receiver<T>>>)>()
        {
            Some(&(ref mut senders, ref mut receivers)) =>
            {
                self.allocated += 1;
                (senders.clone(), receivers[self.index].take())
            }
            _ => { panic!("unable to cast channel correctly"); }
        }
    }

    pub fn new_vector(multiplicity: uint) -> Vec<ChannelAllocator>
    {
        let channels = Arc::new(Mutex::new(Vec::new()));

        range(0, multiplicity).map(|index| ChannelAllocator
        {
            index: index,
            multiplicity: multiplicity,
            allocated: 0,
            channels: channels.clone(),
        }).collect()
    }
}
