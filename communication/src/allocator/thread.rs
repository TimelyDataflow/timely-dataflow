//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};

use crate::allocator::{Allocate, AllocateBuilder};
use crate::allocator::counters::Pusher as CountPusher;
use crate::allocator::counters::Puller as CountPuller;
use crate::{Push, Pull};

/// Builder for single-threaded allocator.
pub struct ThreadBuilder;

impl AllocateBuilder for ThreadBuilder {
    type Allocator = Thread;
    fn build(self) -> Self::Allocator { Thread::default() }
}


/// An allocator for intra-thread communication.
#[derive(Default)]
pub struct Thread {
    /// Shared counts of messages in channels.
    events: Rc<RefCell<Vec<usize>>>,
    /// Reusable values shared by all pipeline channels allocated by this worker.
    recycler: RecyclerHandle,
}

impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {
        let (pusher, puller) = Thread::new_from_with_recycler(
            identifier,
            Rc::clone(&self.events),
            Rc::clone(&self.recycler),
        );
        (vec![Box::new(pusher)], Box::new(puller))
    }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
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
pub type ThreadPusher<T> = CountPusher<T, Pusher<T>>;
/// Thread-local counting channel pull endpoint.
pub type ThreadPuller<T> = CountPuller<T, Puller<T>>;

impl Thread {
    /// Creates a new thread-local channel from an identifier and shared counts.
    pub fn new_from<T: 'static>(identifier: usize, events: Rc<RefCell<Vec<usize>>>)
        -> (ThreadPusher<T>, ThreadPuller<T>)
    {
        Self::new_from_with_recycler(identifier, events, Default::default())
    }

    pub(crate) fn new_from_with_recycler<T: 'static>(
        identifier: usize,
        events: Rc<RefCell<Vec<usize>>>,
        recycler: RecyclerHandle,
    ) -> (ThreadPusher<T>, ThreadPuller<T>) {
        let shared = Rc::new(RefCell::new(VecDeque::<T>::new()));
        let pool = recycler.borrow_mut().pool::<T>();
        let pusher = Pusher {
            target: Rc::clone(&shared),
            pool: Rc::clone(&pool),
        };
        let pusher = CountPusher::new(pusher, identifier, Rc::clone(&events));
        let puller = Puller { source: shared, current: None, pool };
        let puller = CountPuller::new(puller, identifier, events);
        (pusher, puller)
    }

    pub(crate) fn recycler(&self) -> RecyclerHandle {
        Rc::clone(&self.recycler)
    }
}

/// Maximum number of reusable values retained for each concrete channel type.
const PER_TYPE_LIMIT: usize = 2;

pub(crate) type RecyclerHandle = Rc<RefCell<Recycler>>;

/// Worker-owned storage shared by pipeline channels of the same concrete type.
#[derive(Default)]
pub(crate) struct Recycler {
    pools: HashMap<TypeId, Box<dyn Any>>,
}

impl Recycler {
    fn pool<T: 'static>(&mut self) -> Pool<T> {
        Rc::clone(self
            .pools
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(Pool::<T>::default()))
            .downcast_ref::<Pool<T>>()
            .expect("TypeId must identify the recycler pool type"))
    }
}

type Pool<T> = Rc<RefCell<Vec<T>>>;

fn recycle<T>(pool: &Pool<T>, value: T) -> Result<(), T> {
    let mut pool = pool.borrow_mut();
    if pool.len() < PER_TYPE_LIMIT {
        pool.push(value);
        Ok(())
    } else {
        Err(value)
    }
}


/// The push half of an intra-thread channel.
pub struct Pusher<T> {
    target: Rc<RefCell<VecDeque<T>>>,
    pool: Pool<T>,
}

impl<T: 'static> Push<T> for Pusher<T> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        let sent = element.is_some();
        if let Some(element) = element.take() {
            self.target.borrow_mut().push_back(element);
        }
        // `Push::done` calls `push` with `None`; do not remove and immediately
        // drop a pooled value merely to communicate that control signal.
        if sent {
            *element = self.pool.borrow_mut().pop();
        }
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T: 'static> {
    current: Option<T>,
    source: Rc<RefCell<VecDeque<T>>>,
    pool: Pool<T>,
}

impl<T: 'static> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        if let Some(element) = self.current.take() {
            let _ = recycle(&self.pool, element);
        }
        self.current = self.source.borrow_mut().pop_front();
        &mut self.current
    }
}

impl<T: 'static> Drop for Puller<T> {
    fn drop(&mut self) {
        if let Some(element) = self.current.take() {
            let _ = recycle(&self.pool, element);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_of_the_same_type_share_returned_values() {
        let allocator = Thread::default();
        let events = Rc::clone(&allocator.events);
        let recycler = allocator.recycler();
        let (mut first_push, mut first_pull) = Thread::new_from_with_recycler::<Vec<u8>>(
            0,
            Rc::clone(&events),
            Rc::clone(&recycler),
        );
        let (mut second_push, _second_pull) =
            Thread::new_from_with_recycler::<Vec<u8>>(1, events, recycler);

        let value = Vec::with_capacity(1024);
        let allocation = value.as_ptr();
        let mut slot = Some(value);
        first_push.push(&mut slot);
        assert!(slot.is_none());

        first_pull.pull().as_mut().unwrap().clear();
        assert!(first_pull.pull().is_none());

        let mut second = Some(Vec::new());
        second_push.push(&mut second);
        let returned = second.expect("second channel should acquire the returned value");
        assert_eq!(returned.as_ptr(), allocation);
        assert_eq!(returned.capacity(), 1024);
    }

    #[test]
    fn recycler_bounds_each_concrete_type() {
        let mut recycler = Recycler::default();
        let bytes = recycler.pool::<Vec<u8>>();
        assert!(recycle(&bytes, Vec::new()).is_ok());
        assert!(recycle(&bytes, Vec::new()).is_ok());
        assert!(recycle(&bytes, Vec::new()).is_err());

        let words = recycler.pool::<Vec<u64>>();
        assert!(recycle(&words, Vec::new()).is_ok());
        assert!(recycle(&words, Vec::new()).is_ok());
        assert!(recycle(&words, Vec::new()).is_err());
    }

    #[test]
    fn done_does_not_discard_a_recycled_value() {
        let recycler = RecyclerHandle::default();
        let pool = recycler.borrow_mut().pool::<Vec<u8>>();
        recycle(&pool, Vec::with_capacity(1024)).unwrap();
        let (mut push, _pull) = Thread::new_from_with_recycler::<Vec<u8>>(
            0,
            Rc::new(RefCell::new(Vec::new())),
            Rc::clone(&recycler),
        );

        push.done();

        assert_eq!(pool.borrow_mut().pop().unwrap().capacity(), 1024);
    }
}
