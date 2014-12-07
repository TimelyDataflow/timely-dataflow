use std::rc::Rc;
use std::cell::RefCell;

use progress::timestamp::Timestamp;

// a port from which data can be received
pub trait SourcePort<T: Timestamp, D: Copy> : 'static
{
    fn register_interest(&mut self, target: Box<TargetPort<T, D>>) -> ();
}

// a port into which data can be sent
pub trait TargetPort<T: Timestamp, D: Copy> : 'static
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>) -> ();
}

// a default implementation for a ref-counted vector of boxed target ports.
impl<T: Timestamp, D: Copy> TargetPort<T, D> for Rc<RefCell<Vec<Box<TargetPort<T, D>>>>>
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>) -> ()
    {
        for target in self.borrow_mut().iter_mut()
        {
            target.deliver_data(time, data);
        }
    }
}

// a default implementation for a ref-counted vector of boxed target ports.
impl<T: Timestamp, D: Copy> SourcePort<T, D> for Rc<RefCell<Vec<Box<TargetPort<T, D>>>>>
{
    fn register_interest(&mut self, target: Box<TargetPort<T, D>>) -> ()
    {
        self.borrow_mut().push(target);
    }
}
