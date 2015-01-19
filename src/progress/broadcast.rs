use std::default::Default;
use std::sync::mpsc::{Sender, Receiver};

use progress::Timestamp;
use communication::ChannelAllocator;

pub type ProgressVec<T> = Vec<(u64, u64, T, i64)>;  // (scope, [in/out]port, timestamp, delta)

// a mechanism for broadcasting progress information within a scope
// broadcasts contents of update, repopulates from broadcasts
pub trait ProgressBroadcaster<T:Timestamp> : 'static {
    fn send_and_recv(&mut self, messages: &mut ProgressVec<T>, internal: &mut ProgressVec<T>) -> ();
}


// enum of known broadcasters, to avoid generics and trait objects
pub enum Progcaster<T:Timestamp> {
    Local,
    Process(MultiThreadedBroadcaster<T>),
}

impl<T:Timestamp> ProgressBroadcaster<T> for Progcaster<T> {
    fn send_and_recv(&mut self, messages: &mut ProgressVec<T>, internal: &mut ProgressVec<T>) -> () {
        match *self {
            Progcaster::Local              => {},   // does no broadcasting; returns submitted updates.
            Progcaster::Process(ref mut b) => b.send_and_recv(messages, internal),
        }
    }
}

impl<T:Timestamp> Default for Progcaster<T> {
    fn default() -> Progcaster<T> { Progcaster::Local }
}


pub struct MultiThreadedBroadcaster<T:Timestamp> {
    senders:    Vec<Sender<(ProgressVec<T>, ProgressVec<T>)>>,
    receiver:   Receiver<(ProgressVec<T>, ProgressVec<T>)>,
    // queue:      Vec<ProgressVec<T>>,
}

impl<T:Timestamp+Send> ProgressBroadcaster<T> for MultiThreadedBroadcaster<T> {
    fn send_and_recv(&mut self, messages: &mut ProgressVec<T>, internal: &mut ProgressVec<T>) -> () {
        if self.senders.len() > 1 {  // if the length is one, just return the updates...
            if messages.len() > 0 || internal.len() > 0 {
                for sender in self.senders.iter() {
                    sender.send((messages.clone(), internal.clone())).ok().expect("broadcast send error");
                }

                messages.clear();
                internal.clear();
            }

            // let receiver = &self.receiver.as_ref().unwrap();
            while let Ok((mut recv_messages, mut recv_internal)) = self.receiver.try_recv() {
                for update in recv_messages.drain() { messages.push(update); }
                for update in recv_internal.drain() { internal.push(update); }
                // if self.queue.len() < 16 { self.queue.push(recv_messages); }
                // if self.queue.len() < 16 { self.queue.push(recv_internal); }
            }
        }
    }
}

impl<T:Timestamp+Send> MultiThreadedBroadcaster<T> {
    pub fn from(allocator: &mut ChannelAllocator) -> MultiThreadedBroadcaster<T> {
        let (senders, receiver) = allocator.new_channel();
        MultiThreadedBroadcaster {
            senders:    senders,
            receiver:   receiver,
            // queue:      Vec::new(),
        }
    }
}
