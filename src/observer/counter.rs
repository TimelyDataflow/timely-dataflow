use std::rc::Rc;
use std::cell::RefCell;

use progress::CountMap;
use observer::Observer;
use communicator::Message;


pub struct Counter<O: Observer> {
    observer:   O,
    counts:     Rc<RefCell<CountMap<O::Time>>>,
    count:      i64,
}

impl<O: Observer> Observer for Counter<O> where O::Time : Eq+Clone+'static {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.observer.open(time); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        self.counts.borrow_mut().update(time, self.count);
        self.observer.shut(time);
        self.count = 0;
    }
    #[inline(always)] fn give(&mut self, data: &mut Message<O::Data>) {
        self.count += data.len() as i64;
        self.observer.give(data);
    }
}

impl<O: Observer> Counter<O> where O::Time : Eq+Clone+'static {
    pub fn new(observer: O, counts: Rc<RefCell<CountMap<O::Time>>>) -> Counter<O> {
        Counter {
            observer:   observer,
            counts:     counts,
            count:      0,
        }
    }

    #[inline(always)] pub fn pull_progress(&mut self, updates: &mut CountMap<O::Time>) {
        while let Some((ref time, delta)) = self.counts.borrow_mut().pop() { updates.update(time, delta); }
    }
}
