use progress::Timestamp;
use communication::{Communicator, Pushable, Pullable};

pub type ProgressVec<T> = Vec<(u64, u64, T, i64)>;  // (scope, [in/out]port, timestamp, delta)

pub struct Progcaster<T:Timestamp> {
    senders:    Vec<Box<Pushable<(ProgressVec<T>, ProgressVec<T>)>>>,
    receiver:   Box<Pullable<(ProgressVec<T>, ProgressVec<T>)>>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    pub fn new(communicator: &mut Communicator) -> Progcaster<T> {
        let (senders, receiver) = communicator.new_channel();
        Progcaster { senders: senders, receiver: receiver }
    }
    pub fn send_and_recv(&mut self, messages: &mut ProgressVec<T>, internal: &mut ProgressVec<T>) -> () {
        if self.senders.len() > 1 {  // if the length is one, just return the updates...
            if messages.len() > 0 || internal.len() > 0 {
                for sender in self.senders.iter_mut() {
                    sender.push((messages.clone(), internal.clone()));
                }

                messages.clear();
                internal.clear();
            }

            // let receiver = &self.receiver.as_ref().unwrap();
            while let Some((mut recv_messages, mut recv_internal)) = self.receiver.pull() {
                for update in recv_messages.drain() { messages.push(update); }
                for update in recv_internal.drain() { internal.push(update); }
            }
        }
    }
}
