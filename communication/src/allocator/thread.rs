//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::VecDeque;

use crate::allocator::{Allocate, AllocateBuilder, Event};
use crate::allocator::counters::Pusher as CountPusher;
use crate::allocator::counters::Puller as CountPuller;
use crate::{Push, Pull, Message};

/// Builder for single-threaded allocator.
pub struct ThreadBuilder;

impl AllocateBuilder for ThreadBuilder {
    type Allocator = Thread;
    fn build(self) -> Self::Allocator { Thread::new() }
}


/// An allocator for intra-thread communication.
pub struct Thread {
    /// Shared counts of messages in channels.
    events: Rc<RefCell<VecDeque<(usize, Event)>>>,
}

impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static, A: 'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>, Message<A>>>>, Box<dyn Pull<Message<T>, Message<A>>>) {
        let (pusher, puller) = Thread::new_from(identifier, self.events.clone());
        (vec![Box::new(pusher)], Box::new(puller))
    }
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        &self.events
    }
    fn await_events(&self, duration: Option<Duration>) {
        if self.events.borrow().is_empty() {
            if let Some(duration) = duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}

/// Thread-local counting channel push endpoint.
pub type ThreadPusher<T, A> = CountPusher<T, A, Pusher<T, A>>;
/// Thread-local counting channel pull endpoint.
pub type ThreadPuller<T, A> = CountPuller<T, A, Puller<T, A>>;

impl Thread {
    /// Allocates a new thread-local channel allocator.
    pub fn new() -> Self {
        Thread {
            events: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    /// Creates a new thread-local channel from an identifier and shared counts.
    pub fn new_from<T: 'static, A>(identifier: usize, events: Rc<RefCell<VecDeque<(usize, Event)>>>)
        -> (ThreadPusher<Message<T>, A>, ThreadPuller<Message<T>, A>)
    {
        let shared = Rc::new(RefCell::new((VecDeque::<Message<T>>::new(), VecDeque::<A>::new())));
        let pusher = Pusher { target: shared.clone() };
        let pusher = CountPusher::new(pusher, identifier, events.clone());
        let puller = Puller { source: shared, current: (None, None) };
        let puller = CountPuller::new(puller, identifier, events);
        (pusher, puller)
    }
}


/// The push half of an intra-thread channel.
pub struct Pusher<T, A> {
    target: Rc<RefCell<(VecDeque<T>, VecDeque<A>)>>,
}

impl<T, A> Push<T, A> for Pusher<T, A> {
    #[inline]
    fn push(&mut self, element: Option<T>, allocation: &mut Option<A>) {
        let mut borrow = self.target.borrow_mut();
        if let Some(element) = element {
            borrow.0.push_back(element);
        }
        *allocation = borrow.1.pop_front();
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T, A> {
    current: (Option<T>, Option<A>),
    source: Rc<RefCell<(VecDeque<T>, VecDeque<A>)>>,
}

impl<T, A> Pull<T, A> for Puller<T, A> {
    #[inline]
    fn pull(&mut self) -> &mut (Option<T>, Option<A>) {
        let mut borrow = self.source.borrow_mut();
        // if let Some(element) = self.current.take() {
        //     // TODO : Arbitrary constant.
        //     if borrow.1.len() < 16 {
        //         borrow.1.push_back(element);
        //     }
        // }
        self.current.0 = borrow.0.pop_front();
        &mut self.current
    }
}
