//! Broadcasts progress information among workers.

use progress::Timestamp;
use progress::ChangeBatch;
use timely_communication::Allocate;
use {Push, Pull};
use logging::Logger;

/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((usize, usize, T), i64)>;
/// A progress update message consisting of source worker id, sequence number and lists of
/// message and internal updates
pub type ProgressMsg<T> = (usize, usize, ProgressVec<T>, ProgressVec<T>);

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    pushers: Vec<Box<Push<ProgressMsg<T>>>>,
    puller: Box<Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Sequence number counter
    counter: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this subgraph
    addr: Vec<usize>,
    /// Communication channel identifier
    comm_channel: Option<usize>,

    logging: Logger,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied allocator.
    pub fn new<A: Allocate>(allocator: &mut A, path: &Vec<usize>, logging: Logger) -> Progcaster<T> {
        let (pushers, puller, chan) = allocator.allocate();
        logging.when_enabled(|l| l.log(::logging::TimelyEvent::CommChannels(::logging::CommChannelsEvent {
            comm_channel: chan,
            comm_channel_kind: ::logging::CommChannelKind::Progress,
        })));
        let worker = allocator.index();
        let addr = path.clone();
        Progcaster { pushers: pushers, puller: puller, source: worker,
                     counter: 0, addr: addr, comm_channel: chan,
                     logging: logging }
    }

    /// Sends and receives progress updates, broadcasting the contents of `messages` and `internal`,
    /// and updating each with updates from other workers.
    pub fn send_and_recv(
        &mut self,
        messages: &mut ChangeBatch<(usize, usize, T)>,
        internal: &mut ChangeBatch<(usize, usize, T)>)
    {
        if self.pushers.len() > 1 {  // if the length is one, just return the updates...
            if !messages.is_empty() || !internal.is_empty() {
                self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::Progress(::logging::ProgressEvent {
                    is_send: true,
                    source: self.source,
                    comm_channel: self.comm_channel,
                    seq_no: self.counter,
                    addr: self.addr.clone(),
                    // TODO: fill with additional data
                    messages: Vec::new(),
                    internal: Vec::new(),
                })));

                for pusher in self.pushers.iter_mut() {
                    // TODO: This should probably use a broadcast channel, or somehow serialize only once.
                    //       It really shouldn't be doing all of this cloning, that's for sure.
                    pusher.push(&mut Some((self.source, self.counter, messages.clone().into_inner(), internal.clone().into_inner())));
                }

                self.counter += 1;

                messages.clear();
                internal.clear();
            }

            // TODO : Could take ownership, and recycle / reuse for next broadcast ...
            while let Some((ref source, ref counter, ref mut recv_messages, ref mut recv_internal)) = *self.puller.pull() {

                let comm_channel = self.comm_channel;
                let addr = &mut self.addr;
                self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::Progress(::logging::ProgressEvent {
                    is_send: false,
                    source: *source,
                    seq_no: *counter,
                    comm_channel,
                    addr: addr.clone(),
                    // TODO: fill with additional data
                    messages: Vec::new(),
                    internal: Vec::new(),
                })));

                // We clone rather than drain to avoid deserialization.
                for &(ref update, delta) in recv_messages.iter() {
                    messages.update(update.clone(), delta);
                }

                // We clone rather than drain to avoid deserialization.
                for &(ref update, delta) in recv_internal.iter() {
                    internal.update(update.clone(), delta);
                }
            }
        }
    }
}
