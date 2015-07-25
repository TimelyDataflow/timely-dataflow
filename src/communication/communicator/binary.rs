use std::sync::mpsc::{Sender, Receiver, channel};
use std::marker::PhantomData;

use serialization::Serializable;
use networking::networking::MessageHeader;

use communication::communicator::Process;
use communication::{Communicator, Data, Message, Pullable};

// A communicator intended for binary channels (networking, pipes, shared memory)
pub struct Binary {
    pub inner:      Process,    // inner Process (use for process-local channels)
    pub index:      u64,                    // index of this worker
    pub peers:      u64,                    // number of peer workers
    pub graph:      u64,                    // identifier for the current graph
    pub allocated:  u64,                    // indicates how many channels have been allocated (locally).

    // for loading up state in the networking threads.
    pub writers:    Vec<Sender<((u64, u64, u64), Sender<Vec<u8>>)>>,
    pub readers:    Vec<Sender<((u64, u64, u64), (Sender<Vec<u8>>, Receiver<Vec<u8>>))>>,
    pub senders:    Vec<Sender<(MessageHeader, Vec<u8>)>>
}

impl Binary {
    pub fn inner<'a>(&'a mut self) -> &'a mut Process { &mut self.inner }
}

// A Communicator backed by Sender<Vec<u8>>/Receiver<Vec<u8>> pairs (e.g. networking, shared memory, files, pipes)
impl Communicator for Binary {
    fn index(&self) -> u64 { self.index }
    fn peers(&self) -> u64 { self.peers }
    fn new_channel<T:Data+Serializable, D: Data+Serializable>(&mut self) -> (Vec<::communication::observer::BoxedObserver<T, D>>, Box<Pullable<T, D>>) {
        let mut pushers: Vec<::communication::observer::BoxedObserver<T, D>> = Vec::new(); // built-up vector of BoxedObserver<T, D> to return

        // we'll need process-local channels as well (no self-loop binary connection in this design; perhaps should allow)
        let inner_peers = self.inner.peers();
        let (inner_sends, inner_recv) = self.inner.new_channel();

        // prep a pushable for each endpoint, multiplied by inner_peers
        for (index, writer) in self.writers.iter().enumerate() {
            for counter in (0..inner_peers) {
                let (s,_r) = channel();  // TODO : Obviously this should be deleted...
                let mut target_index = index as u64 * inner_peers + counter as u64;

                // we may need to increment target_index by inner_peers;
                if index as u64 >= self.index / inner_peers { target_index += inner_peers; }

                writer.send(((self.index, self.graph, self.allocated), s)).unwrap();
                let header = MessageHeader {
                    graph:      self.graph,     // should be
                    channel:    self.allocated, //
                    source:     self.index,     //
                    target:     target_index,   //
                    length:     0,
                };
                pushers.push(::communication::observer::BoxedObserver::new(Observer::new(header, self.senders[index].clone())));
            }
        }

        // splice inner_sends into the vector of pushables
        for (index, writer) in inner_sends.into_iter().enumerate() {
            pushers.insert(((self.index / inner_peers) * inner_peers) as usize + index, writer);
        }

        // prep a Box<Pullable<T>> using inner_recv and fresh registered pullables
        let (send,recv) = channel();    // binary channel from binary listener to BinaryPullable<T>
        let mut pullsends = Vec::new();
        for reader in self.readers.iter() {
            let (s,r) = channel();
            pullsends.push(s);
            reader.send(((self.index, self.graph, self.allocated), (send.clone(), r))).unwrap();
        }

        let pullable = Box::new(BinaryPullable {
            inner: inner_recv,
            current: None,
            receiver: recv,
        });

        self.allocated += 1;

        return (pushers, pullable);
    }
}

struct Observer<T, D> {
    header:     MessageHeader,
    sender:     Sender<(MessageHeader, Vec<u8>)>,   // targets for each remote destination
    phantom:    PhantomData<D>,
    time: Option<T>,
}

impl<T, D> Observer<T, D> {
    pub fn new(header: MessageHeader, sender: Sender<(MessageHeader, Vec<u8>)>) -> Observer<T, D> {
        Observer {
            header:     header,
            sender:     sender,
            phantom:    PhantomData,
            time: None,
        }
    }
}

impl<T:Data+Serializable, D:Data+Serializable> ::communication::observer::Observer for Observer<T, D> {
    type Time = T;
    type Data = D;

    #[inline] fn open(&mut self, time: &Self::Time) {
        assert!(self.time.is_none());
        self.time = Some(time.clone());
    }
    #[inline] fn shut(&mut self,_time: &Self::Time) {
        assert!(self.time.is_some());
        self.time = None;
    }
    #[inline] fn give(&mut self, data: &mut Message<Self::Data>) {
        assert!(self.time.is_some());
        if let Some(mut time) = self.time.clone() {
            // TODO : anything better to do here than allocate (bytes)?
            // TODO : perhaps team up with the Pushable to recycle (bytes) ...

            // VERY IMPORTANT : We don't know much about data. If it is binary, we believe that
            // it is valid, but we don't know that the encoded timestamp is self.time, for example.
            // We could try and tweak the layout (putting time last, for example) to allow a faster
            // serialization process, but it is something to be done carefully.

            // For now, just serialize the whole thing and think about optimizations later on.
            // Also, who serialized data and now wants to re-serialize it without breaking it up
            // via an exchange channel? Seems uncommon.

            let mut bytes = Vec::new();
            <T as Serializable>::encode(&mut time, &mut bytes);
            <Vec<D> as Serializable>::encode(data.look(), &mut bytes);
            // data.clear();   // <--- ??????? improve!!!

            let mut header = self.header;
            header.length = bytes.len() as u64;

            self.sender.send((header, bytes)).ok();
        }
    }
}

struct BinaryPullable<T, D> {
    inner: Box<Pullable<T, D>>,       // inner pullable (e.g. intra-process typed queue)
    current: Option<(T, Message<D>)>,
    receiver: Receiver<Vec<u8>>,      // source of serialized buffers
}
impl<T:Data+Serializable, D: Data+Serializable> BinaryPullable<T, D> {
    fn new(inner: Box<Pullable<T, D>>, receiver: Receiver<Vec<u8>>) -> BinaryPullable<T, D> {
        BinaryPullable {
            inner: inner,
            current: None,
            receiver: receiver,
        }
    }
}

impl<T:Data+Serializable, D: Data+Serializable> Pullable<T, D> for BinaryPullable<T, D> {
    #[inline]
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> {
        if let Some(pair) = self.inner.pull() { Some(pair) }
        else {
            let next = self.receiver.try_recv().ok().map(|mut bytes| {
                let x_len = bytes.len();
                let (time, off, len) = {
                    let (t,r) = <T as Serializable>::decode(&mut bytes).unwrap();
                    let o = x_len - r.len();
                    let l = <Vec<D> as Serializable>::decode(r).unwrap().0.len();
                    ((*t).clone(), o, l)
                };

                (time, Message::Bytes(bytes, off, len))
            });
            let prev = ::std::mem::replace(&mut self.current, next);
            match prev {
                // TODO : Do something better than drop
                Some((_time, Message::Bytes(_bytes, _, _))) => { },
                Some((_time, Message::Typed(_typed))) => { },
                None => { },
            };
            if let Some((_, ref mut message)) = self.current {
                // TODO : old code; can't recall why this would happen.
                // TODO : probably shouldn't, but I recall a progress
                // TODO : tracking issue if it ever did. check it out!
                // TODO : many operators will call notify_at if they get any messages, is why!
                assert!(message.len() > 0);
            }
            self.current.as_mut().map(|&mut (ref time, ref mut data)| (time, data))
        }
    }
}
