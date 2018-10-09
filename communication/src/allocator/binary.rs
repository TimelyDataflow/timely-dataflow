use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::Arc;

use {Allocate, Data, Push, Pull};
use allocator::{Message, Process};
use networking::MessageHeader;

// A communicator intended for binary channels (networking, pipes, shared memory)
pub struct Binary {
    pub inner:      Process,    // inner Process (use for process-local channels)
    pub index:      usize,      // index of this worker
    pub peers:      usize,      // number of peer workers

    // for loading up state in the networking threads.
    pub readers:    Vec<Sender<((usize, usize), Sender<Vec<u8>>)>>,
    pub senders:    Vec<Sender<(MessageHeader, Vec<u8>)>>,
    pub log_sender: Arc<Fn(::logging::CommsSetup)->::logging::CommsLogger+Send+Sync>,
}

impl Binary {
    pub fn inner<'a>(&'a mut self) -> &'a mut Process { &mut self.inner }
}

// A Communicator backed by Sender<Vec<u8>>/Receiver<Vec<u8>> pairs (e.g. networking, shared memory, files, pipes)
impl Allocate for Binary {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {
        let mut pushers: Vec<Box<Push<Message<T>>>> = Vec::new();

        // we'll need process-local channels as well (no self-loop binary connection in this design; perhaps should allow)
        let inner_peers = self.inner.peers();
        let (inner_sends, inner_recv) = self.inner.allocate(identifier);

        // prep a pushable for each endpoint, multiplied by inner_peers
        for index in 0..self.readers.len() {
            for counter in 0..inner_peers {
                let mut target_index = index * inner_peers + counter;

                // we may need to increment target_index by inner_peers;
                if index >= self.index / inner_peers { target_index += inner_peers; }

                let header = MessageHeader {
                    channel:    identifier,
                    source:     self.index,
                    target:     target_index,
                    length:     0,
                    seqno:      0,
                };
                let logger = (self.log_sender)(::logging::CommsSetup {
                    process: self.index,
                    sender: true,
                    remote: Some(target_index),
                });
                pushers.push(Box::new(Pusher::new(header, self.senders[index].clone(), logger)));
            }
        }

        // splice inner_sends into the vector of pushables
        for (index, writer) in inner_sends.into_iter().enumerate() {
            pushers.insert(((self.index / inner_peers) * inner_peers) + index, writer);
        }

        // prep a Box<Pullable<T>> using inner_recv and fresh registered pullables
        let (send,recv) = channel();    // binary channel from binary listener to BinaryPullable<T>
        for reader in self.readers.iter() {
            reader.send(((self.index, self.allocated), send.clone())).unwrap();
        }

        let logger = (self.log_sender)(::logging::CommsSetup {
            process: self.index,
            sender: false,
            remote: None,
        });
        let pullable = Box::new(Puller::<T>::new(inner_recv, recv, logger)) as Box<Pull<Message<T>>>;

        (pushers, pullable)
    }
}

struct Pusher<T> {
    header:     MessageHeader,
    sender:     Sender<(MessageHeader, Vec<u8>)>,   // targets for each remote destination
    phantom:    ::std::marker::PhantomData<T>,
    log_sender: ::logging::CommsLogger,
}

impl<T> Pusher<T> {
    pub fn new(header: MessageHeader, sender: Sender<(MessageHeader, Vec<u8>)>, log_sender: ::logging::CommsLogger) -> Pusher<T> {
        Pusher {
            header:     header,
            sender:     sender,
            phantom:    ::std::marker::PhantomData,
            log_sender: log_sender,
        }
    }
}

impl<T:Data> Push<Message<T>> for Pusher<T> {
    #[inline] fn push(&mut self, element: &mut Option<Message<T>>) {
        if let Some(ref mut element) = *element {

            self.log_sender.when_enabled(|l| l.log(::logging::CommsEvent::Serialization(::logging::SerializationEvent {
                    seq_no: Some(self.header.seqno),
                    is_start: true,
                })));

            let mut bytes = Vec::new();
            element.into_bytes(&mut bytes);
            // match element {
            //     Message::Binary(b) => bytes.extend(b.as_bytes().iter().cloned()),
            //     Message::Typed(t) => t.into_bytes(&mut bytes),
            // };
            let mut header = self.header;
            header.length = bytes.len();
            self.sender.send((header, bytes)).ok();     // TODO : should be unwrap()?

            self.log_sender.when_enabled(|l| l.log(::logging::CommsEvent::Serialization(::logging::SerializationEvent {
                    seq_no: Some(self.header.seqno),
                    is_start: false,
                })));
            self.header.seqno += 1;
        }
    }
}

struct Puller<T> {
    inner: Box<Pull<Message<T>>>,            // inner pullable (e.g. intra-process typed queue)
    current: Option<Message<T>>,
    receiver: Receiver<Vec<u8>>,    // source of serialized buffers
    log_sender: ::logging::CommsLogger,
}
impl<T:Data> Puller<T> {
    fn new(inner: Box<Pull<Message<T>>>, receiver: Receiver<Vec<u8>>, log_sender: ::logging::CommsLogger) -> Puller<T> {
        Puller { inner: inner, receiver: receiver, current: None, log_sender: log_sender }
    }
}

impl<T:Data> Pull<Message<T>> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<Message<T>> {
        let inner = self.inner.pull();
        let log_sender = &self.log_sender;
        if inner.is_some() { inner }
        else {
            self.current = self.receiver.try_recv().ok().map(|bytes| {
                log_sender.when_enabled(|l| l.log(
                    ::logging::CommsEvent::Serialization(::logging::SerializationEvent {
                        seq_no: None,
                        is_start: true,
                    })));

                let bytes = ::bytes::arc::Bytes::from(bytes);

                log_sender.when_enabled(|l| l.log(
                    ::logging::CommsEvent::Serialization(::logging::SerializationEvent {
                        seq_no: None,
                        is_start: false,
                    })));

                unsafe { Message::from_bytes(bytes) }
            });
            &mut self.current
        }
    }
}
