use std::sync::mpsc::{Sender, Receiver, channel};

use {Allocate, Data, Push, Pull, Serialize};
use allocator::Process;
use networking::MessageHeader;

// A communicator intended for binary channels (networking, pipes, shared memory)
pub struct Binary {
    pub inner:      Process,    // inner Process (use for process-local channels)
    pub index:      usize,      // index of this worker
    pub peers:      usize,      // number of peer workers
    pub allocated:  usize,      // indicates how many channels have been allocated (locally).

    // for loading up state in the networking threads.
    pub readers:    Vec<Sender<((usize, usize), Sender<Vec<u8>>)>>,
    pub senders:    Vec<Sender<(MessageHeader, Vec<u8>)>>
}

impl Binary {
    pub fn inner<'a>(&'a mut self) -> &'a mut Process { &mut self.inner }
}

// A Communicator backed by Sender<Vec<u8>>/Receiver<Vec<u8>> pairs (e.g. networking, shared memory, files, pipes)
impl Allocate for Binary {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T:Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) {
        let mut pushers: Vec<Box<Push<T>>> = Vec::new();

        // we'll need process-local channels as well (no self-loop binary connection in this design; perhaps should allow)
        let inner_peers = self.inner.peers();
        let (inner_sends, inner_recv, _) = self.inner.allocate();

        // prep a pushable for each endpoint, multiplied by inner_peers
        for index in 0..self.readers.len() {
            for counter in 0..inner_peers {
                let mut target_index = index * inner_peers + counter;

                // we may need to increment target_index by inner_peers;
                if index >= self.index / inner_peers { target_index += inner_peers; }

                let header = MessageHeader {
                    channel:    self.allocated,
                    source:     self.index,
                    target:     target_index,
                    length:     0,
                    seqno:      0,
                };
                pushers.push(Box::new(Pusher::new(header, self.senders[index].clone())));
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

        let pullable = Box::new(Puller::new(inner_recv, recv));

        self.allocated += 1;

        return (pushers, pullable, Some(self.allocated - 1));
    }
}

struct Pusher<T> {
    header:     MessageHeader,
    sender:     Sender<(MessageHeader, Vec<u8>)>,   // targets for each remote destination
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T> Pusher<T> {
    pub fn new(header: MessageHeader, sender: Sender<(MessageHeader, Vec<u8>)>) -> Pusher<T> {
        Pusher {
            header:     header,
            sender:     sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data> Push<T> for Pusher<T> {
    #[inline] fn push(&mut self, element: &mut Option<T>) {
        if let Some(ref mut element) = *element {
            ::logging::log(&::logging::SERIALIZATION, ::logging::SerializationEvent {
                seq_no: Some(self.header.seqno),
                is_start: true,
            });
            let mut bytes = Vec::new();
            <T as Serialize>::into_bytes(element, &mut bytes);
            let mut header = self.header;
            header.length = bytes.len();
            self.sender.send((header, bytes)).ok();     // TODO : should be unwrap()?
            ::logging::log(&::logging::SERIALIZATION, ::logging::SerializationEvent {
                seq_no: Some(self.header.seqno),
                is_start: false,
            });
            self.header.seqno += 1;
        }
    }
}

struct Puller<T> {
    inner: Box<Pull<T>>,            // inner pullable (e.g. intra-process typed queue)
    current: Option<T>,
    receiver: Receiver<Vec<u8>>,    // source of serialized buffers
}
impl<T:Data> Puller<T> {
    fn new(inner: Box<Pull<T>>, receiver: Receiver<Vec<u8>>) -> Puller<T> {
        Puller { inner: inner, receiver: receiver, current: None }
    }
}

impl<T:Data> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        let inner = self.inner.pull();
        if inner.is_some() { inner }
        else {
            self.current = self.receiver.try_recv().ok().map(|mut bytes| {
                ::logging::log(&::logging::SERIALIZATION, ::logging::SerializationEvent {
                    seq_no: None,
                    is_start: true,
                });
                let result = <T as Serialize>::from_bytes(&mut bytes);
                ::logging::log(&::logging::SERIALIZATION, ::logging::SerializationEvent {
                    seq_no: None,
                    is_start: false,
                });
                result
            });
            &mut self.current
        }
    }
}
