//! Broadcasts progress information among workers.

use std::rc::Rc;
use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::{Location, Port};
use crate::communication::{Push, Pull};
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;
use crate::Bincode;

/// A progress update message consisting of source worker id, sequence number and lists of
/// message and internal updates
pub type ProgressMsg<T> = Bincode<(usize, usize, ChangeBatch<(Location, T)>)>;

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    /// Pusher into which we send progress updates.
    pusher: Box<dyn Push<ProgressMsg<T>>>,
    /// Puller from which we recv progress updates.
    puller: Box<dyn Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Sequence number counter
    counter: usize,
    /// Global identifier of the scope that owns this `Progcaster`.
    identifier: usize,
    /// Communication channel identifier
    channel_identifier: usize,
    /// An optional logger to record progress messages.
    progress_logging: Option<ProgressLogger<T>>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied worker.
    pub fn new<A: crate::worker::AsWorker>(worker: &mut A, addr: Rc<[usize]>, identifier: usize, mut logging: Option<Logger>, progress_logging: Option<ProgressLogger<T>>) -> Progcaster<T> {

        let channel_identifier = worker.new_identifier();
        let (pusher, puller) = worker.broadcast(channel_identifier, addr);
        logging.as_mut().map(|l| l.log(crate::logging::CommChannelsEvent {
            identifier: channel_identifier,
            kind: crate::logging::CommChannelKind::Progress,
        }));
        let worker_index = worker.index();
        Progcaster {
            pusher,
            puller,
            source: worker_index,
            counter: 0,
            identifier,
            channel_identifier,
            progress_logging,
        }
    }

    /// Sends pointstamp changes to all workers.
    pub fn send(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        changes.compact();
        if !changes.is_empty() {

            self.progress_logging.as_ref().map(|l| {

                // Pre-allocate enough space; we transfer ownership, so there is not
                // an opportunity to re-use allocations (w/o changing the logging
                // interface to accept references).
                let mut messages = Vec::with_capacity(changes.len());
                let mut internal = Vec::with_capacity(changes.len());

                for ((location, time), diff) in changes.iter() {
                    match location.port {
                        Port::Target(port) => {
                            messages.push((location.node, port, time.clone(), *diff))
                        },
                        Port::Source(port) => {
                            internal.push((location.node, port, time.clone(), *diff))
                        }
                    }
                }

                l.log(crate::logging::TimelyProgressEvent {
                    is_send: true,
                    source: self.source,
                    channel: self.channel_identifier,
                    seq_no: self.counter,
                    identifier: self.identifier,
                    messages,
                    internal,
                });
            });

            let payload = (self.source, self.counter, std::mem::take(changes));
            let mut to_push = Some(Bincode { payload });
            self.pusher.push(&mut to_push);
            self.pusher.done();

            if let Some(pushed) = to_push {
                *changes = pushed.payload.2;
                changes.clear();
            }

            self.counter += 1;
        }
    }

    /// Receives pointstamp changes from all workers.
    pub fn recv(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        while let Some(message) = self.puller.pull() {

            let source = message.0;
            let counter = message.1;
            let recv_changes = &mut message.2;

            let channel = self.channel_identifier;

            // See comments above about the relatively high cost of this logging, and our
            // options for improving it if performance limits users who want other logging.
            self.progress_logging.as_ref().map(|l| {

                let mut messages = Vec::with_capacity(changes.len());
                let mut internal = Vec::with_capacity(changes.len());

                for ((location, time), diff) in recv_changes.iter() {

                    match location.port {
                        Port::Target(port) => {
                            messages.push((location.node, port, time.clone(), *diff))
                        },
                        Port::Source(port) => {
                            internal.push((location.node, port, time.clone(), *diff))
                        }
                    }
                }

                l.log(crate::logging::TimelyProgressEvent {
                    is_send: false,
                    source,
                    seq_no: counter,
                    channel,
                    identifier: self.identifier,
                    messages,
                    internal,
                });
            });

            // We clone rather than drain to avoid deserialization.
            for &(ref update, delta) in recv_changes.iter() {
                changes.update(update.clone(), delta);
            }
        }

    }
}
