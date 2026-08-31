//! Push and Pull wrappers to maintain counts of messages in channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::mpsc::Sender;

use crate::{Push, Pull};

/// The push half of an intra-thread channel.
pub struct Pusher<T, P: Push<T>> {
    index: usize,
    pushed: usize,
    events: Rc<RefCell<Vec<usize>>>,
    pusher: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Push<T>>  Pusher<T, P> {
    /// Wraps a pusher with a message counter.
    pub fn new(pusher: P, index: usize, events: Rc<RefCell<Vec<usize>>>) -> Self {
        Pusher {
            index,
            pushed: 0,
            events,
            pusher,
            phantom: ::std::marker::PhantomData,
        }
    }
}

impl<T, P: Push<T>> Push<T> for Pusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        let done = element.is_none();
        self.pusher.push(element);

        // Notify promptly for the first message, and once more at the end
        // if later messages may have arrived after the receiver drained.
        if done {
            if self.pushed > 1 {
                self.events.borrow_mut().push(self.index);
            }
            self.pushed = 0;
        }
        else {
            if self.pushed == 0 {
                self.events.borrow_mut().push(self.index);
            }
            self.pushed = self.pushed.saturating_add(1);
        }
    }
}

/// The push half of an inter-thread channel.
pub struct ArcPusher<T, P: Push<T>> {
    index: usize,
    pushed: usize,
    events: Sender<usize>,
    pusher: P,
    phantom: ::std::marker::PhantomData<T>,
    buzzer: crate::buzzer::Buzzer,
}

impl<T, P: Push<T>>  ArcPusher<T, P> {
    /// Wraps a pusher with a message counter.
    pub fn new(pusher: P, index: usize, events: Sender<usize>, buzzer: crate::buzzer::Buzzer) -> Self {
        ArcPusher {
            index,
            pushed: 0,
            events,
            pusher,
            phantom: ::std::marker::PhantomData,
            buzzer,
        }
    }
}

impl<T, P: Push<T>> Push<T> for ArcPusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        let done = element.is_none();
        self.pusher.push(element);

        // These three calls should happen in this order, to ensure that
        // we first enqueue data, second enqueue interest in the channel,
        // and finally awaken the thread. Other orders are defective when
        // multiple threads are involved.
        if done {
            if self.pushed > 1 {
                let _ = self.events.send(self.index);
                self.buzzer.buzz();
            }
            self.pushed = 0;
        }
        else {
            if self.pushed == 0 {
                let _ = self.events.send(self.index);
                self.buzzer.buzz();
            }
            self.pushed = self.pushed.saturating_add(1);
        }
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T, P: Pull<T>> {
    index: usize,
    count: usize,
    events: Rc<RefCell<Vec<usize>>>,
    puller: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Pull<T>>  Puller<T, P> {
    /// Wraps a puller with a message counter.
    pub fn new(puller: P, index: usize, events: Rc<RefCell<Vec<usize>>>) -> Self {
        Puller {
            index,
            count: 0,
            events,
            puller,
            phantom: ::std::marker::PhantomData,
        }
    }
}
impl<T, P: Pull<T>> Pull<T> for Puller<T, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        let result = self.puller.pull();
        if result.is_none() {
            if self.count != 0 {
                self.events
                    .borrow_mut()
                    .push(self.index);
                self.count = 0;
            }
        }
        else {
            self.count += 1;
        }

        result
    }
}
