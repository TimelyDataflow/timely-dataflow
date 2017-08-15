//! Broadcasts progress information among workers.

use progress::Timestamp;
use progress::ChangeBatch;
use timely_communication::Allocate;
use {Push, Pull};

// TODO : Extend ProgressVec<T> to contain a source worker id and a sequence number.


/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((usize, usize, T), i64)>;

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    pushers: Vec<Box<Push<(ProgressVec<T>, ProgressVec<T>)>>>,
    puller: Box<Pull<(ProgressVec<T>, ProgressVec<T>)>>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied allocator.
    pub fn new<A: Allocate>(allocator: &mut A) -> Progcaster<T> {
        let (pushers, puller) = allocator.allocate();
        Progcaster { pushers: pushers, puller: puller }
    }

    // TODO : puller.pull() forcibly decodes, whereas we would be just as happy to read data from
    // TODO : binary. Investigate fabric::Wrapper for this purpose.
    /// Sends and receives progress updates, broadcasting the contents of `messages` and `internal`,
    /// and updating each with updates from other workers.
    pub fn send_and_recv(
        &mut self,
        messages: &mut ChangeBatch<(usize, usize, T)>,
        internal: &mut ChangeBatch<(usize, usize, T)>)
    {
        if self.pushers.len() > 1 {  // if the length is one, just return the updates...
            if !messages.is_empty() || !internal.is_empty() {
                for pusher in self.pushers.iter_mut() {
                    pusher.push(&mut Some((messages.clone().into_inner(), internal.clone().into_inner())));
                }

                messages.clear();
                internal.clear();
            }

            // TODO : Could take ownership, and recycle / reuse for next broadcast ...
            while let Some((ref mut recv_messages, ref mut recv_internal)) = *self.puller.pull() {

                for (update, delta) in recv_messages.drain(..) {
                    messages.update(update, delta);
                }

                for (update, delta) in recv_internal.drain(..) {
                    internal.update(update, delta);
                }
            }
        }
    }
}
