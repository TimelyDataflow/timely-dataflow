use progress::Timestamp;
use progress::count_map::CountMap;
use communication::{Communicator, Pushable, Pullable};

pub type ProgressVec<T> = Vec<((u64, u64, T), i64)>;  // (child_scope, [in/out]port, timestamp, delta)

pub struct Progcaster<T:Timestamp> {
    senders:    Vec<Box<Pushable<(ProgressVec<T>, ProgressVec<T>)>>>,
    receiver:   Box<Pullable<(ProgressVec<T>, ProgressVec<T>)>>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    pub fn new<C: Communicator>(communicator: &mut C) -> Progcaster<T> {
        let (senders, receiver) = communicator.new_channel();
        Progcaster { senders: senders, receiver: receiver }
    }
    pub fn send_and_recv(&mut self, messages: &mut CountMap<(u64, u64, T)>, internal: &mut CountMap<(u64, u64, T)>) -> () {
        // assert!(messages.elements().iter().all(|x| x.1 != 0));
        // assert!(internal.elements().iter().all(|x| x.1 != 0));
        if self.senders.len() > 1 {  // if the length is one, just return the updates...
            if messages.len() > 0 || internal.len() > 0 {
                for sender in self.senders.iter_mut() {
                    sender.push((messages.elements().clone(), internal.elements().clone()));
                }

                messages.clear();
                internal.clear();
            }

            while let Some((mut recv_messages, mut recv_internal)) = self.receiver.pull() {
                while let Some((update, delta)) = recv_messages.pop() {
                    // println!("messages recv: {:?}: {}", update, delta);
                    messages.update(&update, delta); }
                while let Some((update, delta)) = recv_internal.pop() {
                    // println!("internal recv: {:?}: {}", update, delta);
                    internal.update(&update, delta); }
            }
        }
    }
}
