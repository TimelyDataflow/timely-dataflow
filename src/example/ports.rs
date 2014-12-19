use std::rc::Rc;
use std::cell::RefCell;

use progress::timestamp::Timestamp;

// a port into which data can be sent
pub trait TargetPort<T: Timestamp, D: Copy> : 'static
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>) -> ();
    fn flush(&mut self) -> ();
}

#[deriving(Default)]
pub struct TeePort<T: Timestamp, D: Copy+'static>
{
    pub targets:    Rc<RefCell<Vec<Box<TargetPort<T, D>>>>>,
    pub updates:    Rc<RefCell<Vec<(T, i64)>>>,
}

impl<T: Timestamp, D: Copy+'static> TeePort<T, D>
{
    pub fn new() -> TeePort<T, D>
    {
        TeePort { targets: Rc::new(RefCell::new(Vec::new())), updates: Rc::new(RefCell::new(Vec::new())), }
    }
}

impl<T: Timestamp, D: Copy+'static> Clone for TeePort<T, D>
{
    fn clone(&self) -> TeePort<T, D> { TeePort{ targets: self.targets.clone(), updates: self.updates.clone() } }
}

// a default implementation for a ref-counted vector of boxed target ports.
impl<T: Timestamp, D: Copy+'static> TeePort<T, D>
{
    pub fn deliver_data(&self, time: &T, data: &Vec<D>) -> ()
    {
        self.updates.borrow_mut().push((*time, data.len() as i64));

        for target in self.targets.borrow_mut().iter_mut()
        {
            target.deliver_data(time, data);
        }
    }

    pub fn flush(&self) -> ()
    {
        for target in self.targets.borrow_mut().iter_mut()
        {
            target.flush();
        }
    }
}
