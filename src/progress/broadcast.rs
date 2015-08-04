use progress::Timestamp;
use progress::count_map::CountMap;
use fabric::{Allocate, Push, Pull};

// ((child_scope, [in/out]_port, timestamp), delta)
pub type ProgressVec<T> = Vec<((usize, usize, T), i64)>;

pub struct Progcaster<T:Timestamp> {
    pushers: Vec<Box<Push<(ProgressVec<T>, ProgressVec<T>)>>>,
    puller: Box<Pull<(ProgressVec<T>, ProgressVec<T>)>>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    pub fn new<A: Allocate>(allocator: &mut A) -> Progcaster<T> {
        let (pushers, puller) = allocator.allocate();
        Progcaster { pushers: pushers, puller: puller }
    }

    // TODO : puller.pull() forcibly decodes, whereas we would be just as happy to read data from
    // TODO : binary. Investigate fabric::Wrapper for this purpose.
    pub fn send_and_recv(&mut self, messages: &mut CountMap<(usize, usize, T)>, internal: &mut CountMap<(usize, usize, T)>) -> () {

        // assert!(messages.elements().iter().all(|x| x.1 != 0));
        // assert!(internal.elements().iter().all(|x| x.1 != 0));
        if self.pushers.len() > 1 {  // if the length is one, just return the updates...
            if messages.len() > 0 || internal.len() > 0 {
                for pusher in self.pushers.iter_mut() {
                    // TODO : Feels like an Arc might be not horrible here... less allocation,
                    // TODO : at least, but more "contention" in the deallocation.
                    pusher.push(&mut Some((messages.elements().clone(), internal.elements().clone())));
                }

                messages.clear();
                internal.clear();
            }

            // TODO : Could take ownership, and recycle / reuse for next broadcast ...
            while let Some((ref recv_messages, ref recv_internal)) = *self.puller.pull() {
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
