//! Push and Pull wrappers to maintain counts of messages in channels.

use std::rc::Rc;
use std::cell::RefCell;

use {Push, Pull};

/// The push half of an intra-thread channel.
pub struct Pusher<T, P: Push<T>> {
    index: usize,
    count: i64,
    counts: Rc<RefCell<Vec<(usize, i64)>>>,
    pusher: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Push<T>>  Pusher<T, P> {
    /// Wraps a pusher with a message counter.
    pub fn new(pusher: P, index: usize, counts: Rc<RefCell<Vec<(usize, i64)>>>) -> Self {
        Pusher {
            index,
            count: 0,
            counts,
            pusher,
            phantom: ::std::marker::PhantomData,
        }
    }
}

impl<T, P: Push<T>> Push<T> for Pusher<T, P> {
    #[inline(always)]
    fn push(&mut self, element: &mut Option<T>) {
        if element.is_none() {
            if self.count != 0 {
                self.counts.borrow_mut().push((self.index, self.count));
                self.count = 0;
            }
        }
        else {
            self.count += 1;
        }
        self.pusher.push(element)
    }
}

use std::sync::mpsc::Sender;

/// The push half of an intra-thread channel.
pub struct ArcPusher<T, P: Push<T>> {
    index: usize,
    count: i64,
    counts: Sender<(usize, i64)>,
    pusher: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Push<T>>  ArcPusher<T, P> {
    /// Wraps a pusher with a message counter.
    pub fn new(pusher: P, index: usize, counts: Sender<(usize, i64)>) -> Self {
        ArcPusher {
            index,
            count: 0,
            counts,
            pusher,
            phantom: ::std::marker::PhantomData,
        }
    }
}

impl<T, P: Push<T>> Push<T> for ArcPusher<T, P> {
    #[inline(always)]
    fn push(&mut self, element: &mut Option<T>) {
        if element.is_none() {
            if self.count != 0 {
                self.counts.send((self.index, self.count)).expect("Failed to send message count");
                self.count = 0;
            }
        }
        else {
            self.count += 1;
        }
        self.pusher.push(element)
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T, P: Pull<T>> {
    index: usize,
    count: i64,
    counts: Rc<RefCell<Vec<(usize, i64)>>>,
    puller: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Pull<T>>  Puller<T, P> {
    /// Wraps a puller with a message counter.
    pub fn new(puller: P, index: usize, counts: Rc<RefCell<Vec<(usize, i64)>>>) -> Self {
        Puller {
            index,
            count: 0,
            counts,
            puller,
            phantom: ::std::marker::PhantomData,
        }
    }
}
impl<T, P: Pull<T>> Pull<T> for Puller<T, P> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<T> {
        let result = self.puller.pull();
        if result.is_none() {
            if self.count != 0 {
                self.counts.borrow_mut().push((self.index, self.count));
                self.count = 0;
            }
        }
        else {
            self.count -= 1;
        }
        result
    }
}