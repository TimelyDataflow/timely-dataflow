//! Typed inter-thread, intra-process channels.

use std::sync::{Arc, Mutex};
use std::any::Any;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::HashMap;

use allocator::{Allocate, AllocateBuilder, Message, Thread};
use {Push, Pull};

/// An allocater for inter-thread, intra-process communication
pub struct Process {
    inner: Thread,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap<usize, Box<Any+Send>>>>,
}

impl Process {
    /// Access the wrapped inner allocator.
    pub fn inner<'a>(&'a mut self) -> &'a mut Thread { &mut self.inner }
    /// Allocate a list of connected intra-process allocators.
    pub fn new_vector(count: usize) -> Vec<Process> {
        let channels = Arc::new(Mutex::new(HashMap::new()));
        (0 .. count).map(|index| Process {
            inner:      Thread,
            index:      index,
            peers:      count,
            channels:   channels.clone(),
        }).collect()
    }
}

impl Allocate for Process {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Any+Send+'static>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {

        // ensure exclusive access to shared list of channels
        let mut channels = self.channels.lock().ok().expect("mutex error?");

        let (send, recv, empty) = {

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

            let (send, recv) =
            vector[self.index]
                .take()
                .expect("channel already consumed");

            let empty = vector.iter().all(|x| x.is_none());

            (send, recv, empty)
        };

        if empty { channels.remove(&identifier); }

        let mut temp = Vec::new();
        for s in send.into_iter() { temp.push(Box::new(s) as Box<Push<Message<T>>>); }
        (temp, Box::new(recv) as Box<Pull<super::Message<T>>>)
    }
}

impl AllocateBuilder for Process {
    type Allocator = Self;
    fn build(self) -> Self { self }
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
