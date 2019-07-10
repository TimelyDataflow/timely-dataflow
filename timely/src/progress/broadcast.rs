//! Broadcasts progress information among workers.

use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::Location;
use crate::communication::{Message, Push, Pull};
use crate::logging::TimelyLogger as Logger;

/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((Location, T), i64)>;
/// A progress update message consisting of source worker id, sequence number and lists of
/// message and internal updates
pub type ProgressMsg<T> = Message<(usize, usize, ProgressVec<T>)>;

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    to_push: Option<ProgressMsg<T>>,
    pushers: Vec<Box<dyn Push<ProgressMsg<T>>>>,
    puller: Box<dyn Pull<ProgressMsg<T>>>,
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
    pub fn new<A: crate::worker::AsWorker>(worker: &mut A, path: &Vec<usize>, mut logging: Option<Logger>) -> Progcaster<T> {

        let channel_identifier = worker.new_identifier();
        let (pushers, puller) = worker.allocate(channel_identifier, &path[..]);
        logging.as_mut().map(|l| l.log(crate::logging::CommChannelsEvent {
            identifier: channel_identifier,
            kind: crate::logging::CommChannelKind::Progress,
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

    /// Sends pointstamp changes to all workers.
    pub fn send(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        changes.compact();
        if !changes.is_empty() {

            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
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
                    tuple.2.clear(); tuple.2.extend(changes.iter().cloned());
                }
                // If we don't have an allocation ...
                if self.to_push.is_none() {
                    self.to_push = Some(Message::from_typed((
                        self.source,
                        self.counter,
                        changes.clone().into_inner(),
                    )));
                }

                // TODO: This should probably use a broadcast channel.
                pusher.push(&mut self.to_push);
                pusher.done();
            }

            self.counter += 1;
            changes.clear();
        }
    }

    /// Receives pointstamp changes from all workers.
    pub fn recv(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        while let Some(message) = self.puller.pull() {

            let source = message.0;
            let counter = message.1;
            let recv_changes = &message.2;

            let addr = &mut self.addr;
            let channel = self.channel_identifier;
            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
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
            for &(ref update, delta) in recv_changes.iter() {
                changes.update(update.clone(), delta);
            }
        }

    }
}
