//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use allocator::{Allocate, AllocateBuilder, Message};
use allocator::counters::Pusher as CountPusher;
use allocator::counters::Puller as CountPuller;
use {Push, Pull};

/// Builder for single-threaded allocator.
pub struct ThreadBuilder;

impl AllocateBuilder for ThreadBuilder {
    type Allocator = Thread;
    fn build(self) -> Self::Allocator { Thread::new() }
}


/// An allocator for intra-thread communication.
pub struct Thread {
    /// Shared counts of messages in channels.
    counts: Rc<RefCell<Vec<(usize, i64)>>>,
}

impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {
        let (pusher, puller) = Thread::new_from(identifier, self.counts.clone());
        (vec![Box::new(pusher)], Box::new(puller))
    }
    fn counts(&self) -> &Rc<RefCell<Vec<(usize, i64)>>> {
        &self.counts
    }
}

/// Thread-local counting channel push endpoint.
pub type ThreadPusher<T> = CountPusher<T, Pusher<T>>;
/// Thread-local counting channel pull endpoint.
pub type ThreadPuller<T> = CountPuller<T, Puller<T>>;

impl Thread {
    /// Allocates a new thread-local channel allocator.
    pub fn new() -> Self {
        Thread {
            counts: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Creates a new thread-local channel from an identifier and shared counts.
    pub fn new_from<T: 'static>(identifier: usize, counts: Rc<RefCell<Vec<(usize, i64)>>>)
        -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>)
    {
        let shared = Rc::new(RefCell::new((VecDeque::<Message<T>>::new(), VecDeque::<Message<T>>::new())));
        let pusher = Pusher { target: shared.clone() };
        let pusher = CountPusher::new(pusher, identifier, counts.clone());
        let puller = Puller { source: shared, current: None };
        let puller = CountPuller::new(puller, identifier, counts.clone());
        (pusher, puller)
    }
}


/// The push half of an intra-thread channel.
pub struct Pusher<T> {
    target: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Push<T> for Pusher<T> {
    #[inline(always)]
    fn push(&mut self, element: &mut Option<T>) {
        let mut borrow = self.target.borrow_mut();
        if let Some(element) = element.take() {
            borrow.0.push_back(element);
        }
        *element = borrow.1.pop_front();
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T> {
    current: Option<T>,
    source: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<T> {
        let mut borrow = self.source.borrow_mut();
        // if let Some(element) = self.current.take() {
        //     // TODO : Arbitrary constant.
        //     if borrow.1.len() < 16 {
        //         borrow.1.push_back(element);
        //     }
        // }
        self.current = borrow.0.pop_front();
        &mut self.current
    }
}
