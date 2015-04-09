use core::fmt::Debug;
use std::any::Any;

use progress::Timestamp;
use progress::count_map::CountMap;

use communication::Observer;

use std::rc::Rc;
use std::cell::RefCell;

pub trait Data : Clone+Send+Debug+Any { }
impl<T: Clone+Send+Debug+Any> Data for T { }


pub struct OutputPort<T: Timestamp, D: Data> {
    shared:    Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>>,
}

impl<T: Timestamp, D: Data> Observer for OutputPort<T, D> {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &T) { for target in self.shared.borrow_mut().iter_mut() { target.open(time); } }
    #[inline(always)] fn push(&mut self, data: &D) { for target in self.shared.borrow_mut().iter_mut() { target.push(data); } }
    #[inline(always)] fn shut(&mut self, time: &T) { for target in self.shared.borrow_mut().iter_mut() { target.shut(time); } }
}

impl<T: Timestamp, D: Data> OutputPort<T, D> {
    pub fn new() -> OutputPort<T, D> {
        OutputPort { shared: Rc::new(RefCell::new(Vec::new())) }
    }
    pub fn add_observer<O: Observer<Time=T, Data=D>+'static>(&mut self, observer: O) {
        self.shared.borrow_mut().push(Box::new(observer));
    }
}

impl<T: Timestamp, D: Data> Clone for OutputPort<T, D> {
    fn clone(&self) -> OutputPort<T, D> { OutputPort { shared: self.shared.clone() } }
}


pub struct ObserverHelper<O: Observer> {
    observer:   O,
    counts:     Rc<RefCell<CountMap<O::Time>>>,
    count:      i64,
}

impl<O: Observer> Observer for ObserverHelper<O> where O::Time : Timestamp {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.observer.open(time); }
    #[inline(always)] fn push(&mut self, data: &O::Data) { self.count += 1; self.observer.push(data); }
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
