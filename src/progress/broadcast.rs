//! Broadcasts progress information among workers.

use progress::{ChangeBatch, Timestamp};
use communication::{Message, Push, Pull};
use logging::TimelyLogger as Logger;

/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((usize, usize, T), i64)>;
/// A progress update message consisting of source worker id, sequence number and lists of
/// message and internal updates
pub type ProgressMsg<T> = Message<(usize, usize, ProgressVec<T>, ProgressVec<T>)>;

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    to_push: Option<ProgressMsg<T>>,
    pushers: Vec<Box<Push<ProgressMsg<T>>>>,
    puller: Box<Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Sequence number counter
    counter: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this subgraph
    addr: Vec<usize>,
    /// Communication channel identifier
    channel_identifier: usize,

    logging: Option<Logger>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied worker.
    pub fn new<A: ::worker::AsWorker>(worker: &mut A, path: &Vec<usize>, mut logging: Option<Logger>) -> Progcaster<T> {

        let channel_identifier = worker.new_identifier();
        let (pushers, puller) = worker.allocate(channel_identifier);
        logging.as_mut().map(|l| l.log(::logging::CommChannelsEvent {
            identifier: channel_identifier,
            kind: ::logging::CommChannelKind::Progress,
        }));
        let worker_index = worker.index();
        let addr = path.clone();
        Progcaster {
            to_push: None,
            pushers,
            puller,
            source: worker_index,
            counter: 0,
            addr,
            channel_identifier,
            logging,
        }
    }

    /// Sends and receives progress updates, broadcasting the contents of `messages` and `internal`,
    /// and updating each with updates from other workers.
    pub fn send_and_recv(
        &mut self,
        messages: &mut ChangeBatch<(usize, usize, T)>,
        internal: &mut ChangeBatch<(usize, usize, T)>)
    {
        if self.pushers.len() > 1 {  // if the length is one, just return the updates...

            // ensure minimality.
            messages.compact();
            internal.compact();

            if !messages.is_empty() || !internal.is_empty() {

                self.logging.as_ref().map(|l| l.log(::logging::ProgressEvent {
                    is_send: true,
                    source: self.source,
                    channel: self.channel_identifier,
                    seq_no: self.counter,
                    addr: self.addr.clone(),
                    // TODO: fill with additional data
                    messages: Vec::new(),
                    internal: Vec::new(),
                }));

                for pusher in self.pushers.iter_mut() {

                    // Attempt to re-use allocations, if possible.
                    if let Some(tuple) = &mut self.to_push {
                        let tuple = tuple.as_mut();
                        tuple.0 = self.source;
                        tuple.1 = self.counter;
                        tuple.2.clear(); tuple.2.extend(messages.iter().cloned());
                        tuple.3.clear(); tuple.3.extend(internal.iter().cloned());
                    }
                    // If we don't have an allocation ...
                    if self.to_push.is_none() {
                        self.to_push = Some(Message::from_typed((
                            self.source,
                            self.counter,
                            messages.clone().into_inner(),
                            internal.clone().into_inner(),
                        )));
                    }

                    // TODO: This should probably use a broadcast channel, or somehow serialize only once.
                    //       It really shouldn't be doing all of this cloning, that's for sure.
                    pusher.push(&mut self.to_push);
                }

                self.counter += 1;

                messages.clear();
                internal.clear();
            }

            // TODO : Could take ownership, and recycle / reuse for next broadcast ...
            // while let Some((ref source, ref counter, ref mut recv_messages, ref mut recv_internal)) = *self.puller.pull() {
            while let Some(message) = self.puller.pull() {

                let source = message.0;
                let counter = message.1;
                let recv_messages = &message.2;
                let recv_internal = &message.3;

                let addr = &mut self.addr;
                let channel = self.channel_identifier;
                self.logging.as_ref().map(|l| l.log(::logging::ProgressEvent {
                    is_send: false,
                    source: source,
                    seq_no: counter,
                    channel,
                    addr: addr.clone(),
                    // TODO: fill with additional data
                    messages: Vec::new(),
                    internal: Vec::new(),
                }));

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
