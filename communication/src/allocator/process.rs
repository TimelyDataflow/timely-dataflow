//! Typed inter-thread, intra-process channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::HashMap;

use allocator::thread::{ThreadBuilder};
use allocator::{Allocate, AllocateBuilder, Message, Thread};
use {Push, Pull};

/// An allocater for inter-thread, intra-process communication
pub struct ProcessBuilder {
    inner: ThreadBuilder,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap<usize, Box<Any+Send>>>>,

    counters_send: Vec<Sender<(usize, i64)>>,
    counters_recv: Receiver<(usize, i64)>,
}

impl AllocateBuilder for ProcessBuilder {
    type Allocator = Process;
    fn build(self) -> Self::Allocator {
        Process {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            channels: self.channels,
            counters_send: self.counters_send,
            counters_recv: self.counters_recv,
        }
    }
}

/// An allocater for inter-thread, intra-process communication
pub struct Process {
    inner: Thread,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap<usize, Box<Any+Send>>>>,
    counters_send: Vec<Sender<(usize, i64)>>,
    counters_recv: Receiver<(usize, i64)>,
}

impl Process {
    /// Access the wrapped inner allocator.
    pub fn inner<'a>(&'a mut self) -> &'a mut Thread { &mut self.inner }
    /// Allocate a list of connected intra-process allocators.
    pub fn new_vector(peers: usize) -> Vec<ProcessBuilder> {

        let mut counters_send = Vec::new();
        let mut counters_recv = Vec::new();
        for _ in 0 .. peers {
            let (send, recv) = channel();
            counters_send.push(send);
            counters_recv.push(recv);
        }

        let channels = Arc::new(Mutex::new(HashMap::new()));

        counters_recv
            .into_iter()
            .enumerate()
            .map(|(index, recv)| {
                ProcessBuilder {
                    inner: ThreadBuilder,
                    index,
                    peers,
                    channels: channels.clone(),
                    counters_send: counters_send.clone(),
                    counters_recv: recv,
                }
            })
            .collect()
        // (0 .. count).map(|index| ProcessBuilder {
        //     inner:      ThreadBuilder,
        //     index:      index,
        //     peers:      count,
        //     channels:   channels.clone(),
        // }).collect()
    }
}

impl Allocate for Process {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Any+Send+Sync+'static>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {

        // ensure exclusive access to shared list of channels
        let mut channels = self.channels.lock().ok().expect("mutex error?");

        let (sends, recv, empty) = {

            // we may need to alloc a new channel ...
            let mut entry = channels.entry(identifier).or_insert_with(|| {

                let mut pushers = Vec::new();
                let mut pullers = Vec::new();
                for _ in 0..self.peers {

                    let (s, r): (Sender<Message<T>>, Receiver<Message<T>>) = channel();
                    pushers.push(Pusher { target: s });
                    pullers.push(Puller { source: r, current: None });
                }

                let mut to_box = Vec::new();
                for recv in pullers.into_iter() {
                    to_box.push(Some((pushers.clone(), recv)));
                }

                Box::new(to_box)
            });

            let vector =
            entry
                .downcast_mut::<(Vec<Option<(Vec<Pusher<Message<T>>>, Puller<Message<T>>)>>)>()
                .expect("failed to correctly cast channel");

            let (sends, recv) =
            vector[self.index]
                .take()
                .expect("channel already consumed");

            let empty = vector.iter().all(|x| x.is_none());

            (sends, recv, empty)
        };

        if empty { channels.remove(&identifier); }

        use allocator::counters::ArcPusher as CountPusher;
        use allocator::counters::Puller as CountPuller;

        let sends =
        sends.into_iter()
             .enumerate()
             .map(|(i,s)| CountPusher::new(s, identifier, self.counters_send[i].clone()))
             .map(|s| Box::new(s) as Box<Push<super::Message<T>>>)
             .collect::<Vec<_>>();

        let recv = Box::new(CountPuller::new(recv, identifier, self.inner.counts().clone())) as Box<Pull<super::Message<T>>>;

        (sends, recv)
    }

    fn counts(&self) -> &Rc<RefCell<Vec<(usize, i64)>>> {
        self.inner.counts()
    }

    fn receive(&mut self, mut action: impl FnMut(&[(usize,i64)])) {
        let mut counts = self.inner.counts().borrow_mut();
        while let Ok((index, delta)) = self.counters_recv.try_recv() {
            counts.push((index, delta));
        }

        action(&counts[..]);
        counts.clear();
    }
}

/// The push half of an intra-process channel.
struct Pusher<T> {
    target: Sender<T>,
}

impl<T> Clone for Pusher<T> {
    fn clone(&self) -> Self {
        Pusher { target: self.target.clone() }
    }
}

impl<T> Push<T> for Pusher<T> {
    #[inline] fn push(&mut self, element: &mut Option<T>) {
        if let Some(element) = element.take() {
            self.target.send(element).unwrap();
        }
    }
}

/// The pull half of an intra-process channel.
struct Puller<T> {
    current: Option<T>,
    source: Receiver<T>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        self.current = self.source.try_recv().ok();
        &mut self.current
    }
}
