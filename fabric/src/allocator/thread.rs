use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use allocator::{Allocate, Push, Pull};

// The simplest communicator remains worker-local and just queues sent messages.
pub struct Thread;
impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>) {
        let shared = Rc::new(RefCell::new(VecDeque::<T>::new()));
        (vec![Box::new(Pusher { target: shared.clone() }) as Box<Push<T>>],
         Box::new(Puller { source: shared, current: None }) as Box<Pull<T>>)
    }
}


// an observer wrapping a Rust channel
pub struct Pusher<T> {
    target: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Push<T> for Pusher<T> {
    #[inline] fn push(&mut self, element: &mut Option<T>) {
        if let Some(element) = element.take() {
            self.target.borrow_mut().push_back(element);
        }
    }
}

pub struct Puller<T> {
    current: Option<T>,
    source: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        self.current = self.source.borrow_mut().pop_front();
        &mut self.current
    }
}
