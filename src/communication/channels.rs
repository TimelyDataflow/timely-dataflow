use core::fmt::Show;

use progress::Timestamp;
use progress::count_map::CountMap;

use communication::Observer;

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

pub trait Data : Clone+Send+Show+'static { }
impl<T: Clone+Send+Show+'static> Data for T { }

// pub struct OutputHelper<'a, T: Timestamp, D: Data> {
//     obs:    &'a Vec<Box<Observer<T, D>>>,
//     time:   &'a T,
// }
//
// impl<'a, T: Timestamp, D: Data> OutputBuffer<'a, T, D> {
//     #[inline(always)]
//     pub fn send(&mut self, data: D) -> () {
//         self.parent.buffer.push(data);
//         if self.parent.buffer.len() >= 256 {
//             self.parent.send_buffer(&self.time);
//         }
//     }
// }
//
// #[unsafe_destructor]
// impl<'a, T: Timestamp, D: Data> Drop for OutputBuffer<'a, T, D> {
//     fn drop(&mut self) {
//         self.parent.send_buffer(&self.time);
//         self.parent.done();
//     }
// }

#[derive(Clone)]
pub struct OutputPort<T: Timestamp, D: Data> {
    pub shared:    Rc<RefCell<Vec<Box<Observer<T, D>>>>>,
}


// impl<T: Timestamp, D: Data> OutputPort<T, D> {
//     pub fn new() -> OutputPort<T, D> {
//         OutputPort {
//             shared: Rc::new(RefCell::new(Vec::new())),
//             buffer: Vec::new(),
//         }
//     }
// }
//
//     pub fn buffer_for<'a>(&'a mut self, time: &T) -> OutputBuffer<'a, T, D> {
//         OutputBuffer { parent: self, time: *time, }
//     }
//
//     pub fn pull_progress(&mut self, updates: &mut Vec<(T, i64)>) {
//         for (ref time, delta) in self.updates.borrow_mut().drain() { updates.update(time, delta); }
//     }
//
//     fn send_buffer(&mut self, time: &T) -> () {
//         if self.buffer.len() > 0 {
//             self.updates.borrow_mut().push((*time, self.buffer.len() as i64));
//             for target in self.targets.borrow_mut().iter_mut() {
//                 target.next((*time, self.buffer.clone()));
//             }
//
//             self.buffer.clear();
//         }
//     }
// }

impl<T: Timestamp, D: Data> Observer<T, D> for OutputPort<T, D>
{
    // TODO : Way wrong. borrow once in a RAII helper, once you figure that out.
    fn push(&mut self, data: &D) { for target in self.shared.borrow_mut().iter_mut() { target.push(data); } }
    fn open(&mut self, time: &T) -> () { for target in self.shared.borrow_mut().iter_mut() { target.open(time); } }
    fn shut(&mut self, time: &T) -> () { for target in self.shared.borrow_mut().iter_mut() { target.shut(time); } }
}


pub struct ObserverHelper<T, D, O: Observer<T, D>>
{
    observer:   O,
    counts:     Rc<RefCell<Vec<(T, i64)>>>,
    count:      i64,
}

impl<T:Timestamp, D:Data, O: Observer<T, D>> Observer<T, D> for ObserverHelper<T, D, O>
{
    fn open(&mut self, time: &T) { }
    fn push(&mut self, data: &D) { self.count += 1; self.observer.push(data); }
    fn shut(&mut self, time: &T) -> () {
        self.counts.borrow_mut().update(time, self.count);
        self.observer.shut(time);
        self.count = 0;
    }
}

impl<T: Timestamp, D, O: Observer<T, D>> ObserverHelper<T, D, O>
{
    pub fn new(observer: O, counts: Rc<RefCell<Vec<(T, i64)>>>) -> ObserverHelper<T, D, O> {
        ObserverHelper {
            observer:   observer,
            counts:     counts,
            count:      0,
        }
    }

    pub fn pull_progress(&mut self, updates: &mut Vec<(T, i64)>) {
        for (ref time, delta) in self.counts.borrow_mut().drain() { updates.update(time, delta); }
    }
}
