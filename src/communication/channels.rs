use core::fmt::Show;

use progress::Timestamp;
use progress::count_map::CountMap;

use communication::Observer;

use std::rc::Rc;
use std::cell::RefCell;

pub trait Data : Copy+Clone+Send+Show+'static { }

impl<T: Copy+Clone+Send+Show+'static> Data for T { }

pub struct OutputBuffer<'a, T: Timestamp, D: Data>
{
    parent: &'a mut OutputPort<T, D>,
    time:   T,
}

impl<'a, T: Timestamp, D: Data> OutputBuffer<'a, T, D>
{
    #[inline(always)]
    pub fn send(&mut self, data: D) -> () {
        self.parent.buffer.push(data);
        if self.parent.buffer.len() >= 256 {
            self.parent.send_buffer(&self.time);
        }
    }
}

#[unsafe_destructor]
impl<'a, T: Timestamp, D: Data> Drop for OutputBuffer<'a, T, D>
{
    fn drop(&mut self)
    {
        self.parent.send_buffer(&self.time);
        self.parent.done();
    }
}

#[derive(Default)]
pub struct OutputPort<T: Timestamp, D: Copy+'static>
{
    pub targets:    Rc<RefCell<Vec<Box<Observer<(T, Vec<D>)>>>>>,
    pub updates:    Rc<RefCell<Vec<(T, i64)>>>,
    buffer:         Vec<D>,
}

impl<T: Timestamp, D: Data> OutputPort<T, D>
{
    pub fn new() -> OutputPort<T, D> {
        OutputPort {
            targets:    Rc::new(RefCell::new(Vec::new())),
            updates:    Rc::new(RefCell::new(Vec::new())),
            buffer:     Vec::new(),
        }
    }

    pub fn buffer_for<'a>(&'a mut self, time: &T) -> OutputBuffer<'a, T, D> {
        OutputBuffer {
            parent: self,
            time:   *time,
        }
    }

    pub fn pull_progress(&mut self, updates: &mut Vec<(T, i64)>) {
        let mut my_updates = self.updates.borrow_mut();
        while let Some((time, delta)) = my_updates.pop() {
            updates.update(time, delta);
        }
    }

    fn send_buffer(&mut self, time: &T) -> () {
        if self.buffer.len() > 0 {
            self.updates.borrow_mut().push((*time, self.buffer.len() as i64));
            for target in self.targets.borrow_mut().iter_mut() {
                target.next((*time, self.buffer.clone()));
            }

            self.buffer.clear();
        }
    }
}

impl<T: Timestamp, D: Data> Observer<(T, Vec<D>)> for OutputPort<T, D>
{
    fn next(&mut self, (time, data): (T, Vec<D>)) {
        self.updates.borrow_mut().push((time, data.len() as i64));
        for target in self.targets.borrow_mut().iter_mut() {
            target.next((time, data.clone()));
        }
    }

    fn done(&mut self) -> () {
        for target in self.targets.borrow_mut().iter_mut() { target.done(); }
    }
}

impl<T: Timestamp, D: Data> Clone for OutputPort<T, D>
{
    fn clone(&self) -> OutputPort<T, D>
    {
        OutputPort
        {
            targets: self.targets.clone(),
            updates: self.updates.clone(),
            buffer:  self.buffer.clone(),
        }
    }
}
