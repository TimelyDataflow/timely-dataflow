use std::default::Default;

use progress::Timestamp;

use communication::ChannelAllocator;

use std::sync::mpsc::{Sender, Receiver};

#[derive(Clone, Copy)]
pub enum Progress<T:Timestamp>
{
    MessagesUpdate(u64, u64, T, i64),  // (scope, input port, timestamp, changes)
    FrontierUpdate(u64, u64, T, i64),  // (scope, output port, timestamp, change)
}


// a mechanism for broadcasting progress information within a scope
pub trait ProgressBroadcaster<T:Timestamp> : 'static+Default
{
    // broadcasts contents of update, repopulates from broadcasts
    fn send_and_recv(&mut self, update: &mut Vec<Progress<T>>) -> ();
}

// useful when there is just one thread, and you don't want to do anything.
impl<T:Timestamp> ProgressBroadcaster<T> for ()
{
    fn send_and_recv(&mut self, _updates: &mut Vec<Progress<T>>) -> () { }
}


pub struct MultiThreadedBroadcaster<T:Timestamp>
{
    senders:    Vec<Sender<Vec<Progress<T>>>>,
    receiver:   Option<Receiver<Vec<Progress<T>>>>,

    queue:      Vec<Vec<Progress<T>>>,
}

impl<T:Timestamp+Send+Copy+Clone> ProgressBroadcaster<T> for MultiThreadedBroadcaster<T>
{
    fn send_and_recv(&mut self, updates: &mut Vec<Progress<T>>) -> ()
    {
        // if the length is one, just return the updates...
        if self.senders.len() > 1 {
            if updates.len() > 0 {
                for sender in self.senders.iter() {
                    if let Some(mut vector) = self.queue.pop() {
                        for &update in updates.iter() {
                            vector.push(update);
                        }
                        sender.send(vector).ok().expect("sigh")
                    }
                    else {
                        sender.send(updates.clone()).ok().expect("sigh")
                    }
                }

                updates.clear();
            }

            let receiver = &self.receiver.as_ref().unwrap();
            while let Ok(mut recv_updates) = receiver.try_recv()
            {
                for &update in recv_updates.iter() { updates.push(update); }

                if self.queue.len() < 16
                {
                    recv_updates.clear();
                    self.queue.push(recv_updates);
                }
            }
        }
    }
}

impl<T:Timestamp+Send> Default for MultiThreadedBroadcaster<T>
{
    fn default() -> MultiThreadedBroadcaster<T> { MultiThreadedBroadcaster {senders: vec![], receiver: None, queue: Vec::new() } }
}

impl<T:Timestamp+Send> MultiThreadedBroadcaster<T>
{
    pub fn from(allocator: &mut ChannelAllocator) -> MultiThreadedBroadcaster<T>
    {
        let (senders, receiver) = allocator.new_channel();

        MultiThreadedBroadcaster
        {
            senders:    senders,
            receiver:   receiver,
            queue:      Vec::new(),
        }
    }
}
