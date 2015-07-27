use progress::Timestamp;
use progress::count_map::CountMap;
use communication::{Communicator, Message, Pullable, Observer};
use communication::observer::BoxedObserver;

pub type ProgressVec<T> = Vec<((u64, u64, T), i64)>;  // (child_scope, [in/out]port, timestamp, delta)

pub struct Progcaster<T:Timestamp> {
    senders:    Vec<BoxedObserver<T, (ProgressVec<T>, ProgressVec<T>)>>,
    receiver:   Box<Pullable<T, (ProgressVec<T>, ProgressVec<T>)>>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    pub fn new<C: Communicator>(communicator: &mut C) -> Progcaster<T> {
        let (senders, receiver) = communicator.new_channel();
        Progcaster { senders: senders, receiver: receiver }
    }
    // TODO : This is all a horrible hack to deal with the way channels currently look (with a time)
    pub fn send_and_recv(&mut self, messages: &mut CountMap<(u64, u64, T)>, internal: &mut CountMap<(u64, u64, T)>) -> () {

        // assert!(messages.elements().iter().all(|x| x.1 != 0));
        // assert!(internal.elements().iter().all(|x| x.1 != 0));
        if self.senders.len() > 1 {  // if the length is one, just return the updates...
            if messages.len() > 0 || internal.len() > 0 {
                for sender in self.senders.iter_mut() {
                    sender.open(&Default::default());
                    sender.give(&mut Message::from_typed(&mut vec![(messages.elements().clone(), internal.elements().clone())]));
                    sender.shut(&Default::default());
                }

                messages.clear();
                internal.clear();
            }

            while let Some((_, data)) = self.receiver.pull() {
                let (ref recv_messages, ref recv_internal) = data[0];
                for &(ref update, delta) in recv_messages {
                    messages.update(update, delta);
                }
                for &(ref update, delta) in recv_internal {
                    internal.update(update, delta);
                }
            }
        }
    }
}
