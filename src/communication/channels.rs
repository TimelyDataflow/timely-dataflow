use core::fmt::Show;

use progress::Timestamp;
use progress::count_map::CountMap;

use communication::Observer;

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

pub trait Data : Clone+Send+Show+'static { }
impl<T: Clone+Send+Show+'static> Data for T { }

pub struct OutputPort<T: Timestamp, D: Data> {
    pub shared:    Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>>,
}


impl<T: Timestamp, D: Data> Observer for OutputPort<T, D> {
    type Time = T;
    type Data = D;
    // TODO : Way wrong. borrow once in a RAII helper, once you figure that out.
    fn push(&mut self, data: &D) { for target in self.shared.borrow_mut().iter_mut() { target.push(data); } }
    fn open(&mut self, time: &T) -> () { for target in self.shared.borrow_mut().iter_mut() { target.open(time); } }
    fn shut(&mut self, time: &T) -> () { for target in self.shared.borrow_mut().iter_mut() { target.shut(time); } }
}


pub struct ObserverHelper<O: Observer> {
    observer:   O,
    counts:     Rc<RefCell<Vec<(O::Time, i64)>>>,
    count:      i64,
}

impl<O: Observer> Observer for ObserverHelper<O> where O::Time : Timestamp {
    type Time = O::Time;
    type Data = O::Data;
    fn open(&mut self,_time: &O::Time) { }
    fn push(&mut self, data: &O::Data) { self.count += 1; self.observer.push(data); }
    fn shut(&mut self, time: &O::Time) -> () {
        self.counts.borrow_mut().update(time, self.count);
        self.observer.shut(time);
        self.count = 0;
    }
}

impl<O: Observer> ObserverHelper<O> where O::Time : Eq+Clone+'static {
    pub fn new(observer: O, counts: Rc<RefCell<Vec<(O::Time, i64)>>>) -> ObserverHelper<O> {
        ObserverHelper {
            observer:   observer,
            counts:     counts,
            count:      0,
        }
    }

    pub fn pull_progress(&mut self, updates: &mut Vec<(O::Time, i64)>) {
        for (ref time, delta) in self.counts.borrow_mut().drain() { updates.update(time, delta); }
    }
}
