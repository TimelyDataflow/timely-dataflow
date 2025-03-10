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
// pub type ProgressMsg<T> = Bincode<(usize, usize, ChangeBatch<(Location, T)>)>;

pub type ProgressMsg<T> = crate::dataflow::channels::Message<(), Column<((Location, T), i64)>>;

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

    container: <((Location, T), i64) as columnar::Columnar>::Container,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied worker.
    pub fn new<A: crate::worker::AsWorker>(worker: &mut A, addr: Rc<[usize]>, identifier: usize, mut logging: Option<Logger>, progress_logging: Option<ProgressLogger<T>>) -> Progcaster<T> where <T as columnar::Columnar>::Container: Clone + Send {

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
            container: Default::default(),
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

            use columnar::{Clear, Push};
            self.container.clear();
            for item in changes.drain() {
                self.container.push(&item);
            }
            let message = crate::dataflow::channels::Message {
                time: (),
                data: Column::Typed(std::mem::take(&mut self.container)),
                from: self.source,
                seq: self.counter,
            };
            // self.container.0.push(self.source);
            // self.container.push((self.source, self.counter, &changes.unstable_internal_updates()[..]));

            // let payload = (self.source, self.counter, std::mem::take(changes));
            let mut to_push: Option<ProgressMsg<T>> = Some(message);
            self.pusher.push(&mut to_push);
            self.pusher.done();

            if let Some(pushed) = to_push {
                if let Column::Typed(t) = pushed.data {
                    self.container = t;
                }
                // *changes = pushed.2;
                // changes.clear();
            }

            self.counter += 1;
        }
    }

    /// Receives pointstamp changes from all workers.
    pub fn recv(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        while let Some(message) = self.puller.pull() {

            let source = message.from;
            let counter = message.seq;
            let recv_changes = &mut message.data;

            let channel = self.channel_identifier;

            use columnar::Columnar;
            use timely_container::Container;

            // See comments above about the relatively high cost of this logging, and our
            // options for improving it if performance limits users who want other logging.
            self.progress_logging.as_ref().map(|l| {

                let mut messages = Vec::with_capacity(changes.len());
                let mut internal = Vec::with_capacity(changes.len());

                for ((location, time), diff) in recv_changes.iter() {

                    match location.port {
                        crate::progress::PortReference::Target(port) => {
                            messages.push((location.node, port, T::into_owned(time), *diff))
                        },
                        crate::progress::PortReference::Source(port) => {
                            internal.push((location.node, port, T::into_owned(time), *diff))
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
            for (update, delta) in recv_changes.iter() {
                changes.update(<(Location, T) as Columnar>::into_owned(update), *delta);
            }
        }

    }
}


pub use container::Column;
mod container {

    use columnar::Columnar;
    use columnar::Container as FooBozzle;

    use timely_bytes::arc::Bytes;

    /// A container based on a columnar store, encoded in aligned bytes.
    pub enum Column<C: Columnar> {
        /// The typed variant of the container.
        Typed(C::Container),
        /// The binary variant of the container.
        Bytes(Bytes),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Reasons could include misalignment, cloning of data, or wanting
        /// to release the `Bytes` as a scarce resource.
        Align(Box<[u64]>),
    }

    impl<C: Columnar> Default for Column<C> {
        fn default() -> Self { Self::Typed(Default::default()) }
    }

    impl<C: Columnar> Clone for Column<C> where C::Container: Clone {
        fn clone(&self) -> Self {
            match self {
                Column::Typed(t) => Column::Typed(t.clone()),
                Column::Bytes(b) => {
                    assert!(b.len() % 8 == 0);
                    let mut alloc: Vec<u64> = vec![0; b.len() / 8];
                    bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&b[..]);
                    Self::Align(alloc.into())
                },
                Column::Align(a) => Column::Align(a.clone()),
            }
        }
    }

    use columnar::{Clear, Len, Index, FromBytes};
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;

    use crate::Container;
    impl<C: Columnar> Container for Column<C> {
        fn len(&self) -> usize {
            match self {
                Column::Typed(t) => t.len(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).len(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)).len(),
            }
        }
        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => *self = Column::Typed(Default::default()),
                Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }

        type ItemRef<'a> = C::Ref<'a>;
        type Iter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn iter<'a>(&'a self) -> Self::Iter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_index_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_index_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_index_iter(),
            }
        }

        type Item<'a> = C::Ref<'a>;
        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_index_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_index_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_index_iter(),
            }
        }
    }

    use crate::container::SizableContainer;
    impl<C: Columnar> SizableContainer for Column<C> {
        fn at_capacity(&self) -> bool {
            match self {
                Self::Typed(t) => {
                    let length_in_bytes = 8 * Indexed::length_in_words(&t.borrow());
                    length_in_bytes >= (1 << 20)
                },
                Self::Bytes(_) => true,
                Self::Align(_) => true,
            }
        }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
    }

    use crate::container::PushInto;
    impl<C: Columnar, T> PushInto<T> for Column<C> where C::Container: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            use columnar::Push;
            match self {
                Column::Typed(t) => t.push(item),
                Column::Align(_) | Column::Bytes(_) => {
                    // We really oughtn't be calling this in this case.
                    // We could convert to owned, but need more constraints on `C`.
                    unimplemented!("Pushing into Column::Bytes without first clearing");
                }
            }
        }
    }

    impl<C: Columnar> crate::dataflow::channels::ContainerBytes for Column<C> {
        fn from_bytes(bytes: crate::bytes::arc::Bytes) -> Self {
            // Our expectation / hope is that `bytes` is `u64` aligned and sized.
            // If the alignment is borked, we can relocate. IF the size is borked,
            // not sure what we do in that case.
            assert!(bytes.len() % 8 == 0);
            if let Ok(_) = bytemuck::try_cast_slice::<_, u64>(&bytes) {
                Self::Bytes(bytes)
            }
            else {
                println!("Re-locating bytes for alignment reasons");
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                Self::Align(alloc.into())
            }
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Column::Typed(t) => 8 * Indexed::length_in_words(&t.borrow()),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
            }
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => { Indexed::write(writer, &t.borrow()).unwrap() },
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
            }
        }
    }
}
