use std::fmt::Debug;
use std::any::Any;

use progress::Timestamp;
use progress::count_map::CountMap;

use communication::Observer;

use std::rc::Rc;
use std::cell::RefCell;

pub trait Data : Clone+Send+Debug+Any { }
impl<T: Clone+Send+Debug+Any> Data for T { }

pub struct ObserverHelper<O: Observer> {
    observer:   O,
    counts:     Rc<RefCell<CountMap<O::Time>>>,
    count:      i64,
}

impl<O: Observer> Observer for ObserverHelper<O> where O::Time : Timestamp {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.observer.open(time); }
    #[inline(always)] fn show(&mut self, data: &O::Data) { self.count += 1; self.observer.show(data); }
    #[inline(always)] fn give(&mut self, data:  O::Data) { self.count += 1; self.observer.give(data); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        self.counts.borrow_mut().update(time, self.count);
        self.observer.shut(time);
        self.count = 0;
    }
}

impl<O: Observer> ObserverHelper<O> where O::Time : Eq+Clone+'static {
    pub fn new(observer: O, counts: Rc<RefCell<CountMap<O::Time>>>) -> ObserverHelper<O> {
        ObserverHelper {
            observer:   observer,
            counts:     counts,
            count:      0,
        }
    }

    #[inline(always)] pub fn pull_progress(&mut self, updates: &mut CountMap<O::Time>) {
        while let Some((ref time, delta)) = self.counts.borrow_mut().pop() { updates.update(time, delta); }
    }
}
