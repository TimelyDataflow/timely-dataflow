use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use allocator::Allocate;
use {Push, Pull};


// The simplest communicator remains worker-local and just queues sent messages.
pub struct Thread;
impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>, Option<usize>) {
        let (pusher, puller) = Thread::new();
        (vec![Box::new(pusher)], Box::new(puller), None)
    }
}

impl Thread {
    pub fn new<T: 'static>() -> (Pusher<T>, Puller<T>) {
        let shared = Rc::new(RefCell::new((VecDeque::<T>::new(), VecDeque::<T>::new())));
        (Pusher { target: shared.clone() }, Puller { source: shared, current: None })
    }  
}


// an observer wrapping a Rust channel
pub struct Pusher<T> {
    target: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Push<T> for Pusher<T> {
    #[inline(always)]
    fn push(&mut self, element: &mut Option<T>) {
        let mut borrow = self.target.borrow_mut();
        if let Some(element) = element.take() {
            borrow.0.push_back(element);
        }
        *element = borrow.1.pop_front();
    }
}

pub struct Puller<T> {
    current: Option<T>,
    source: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<T> {
        let mut borrow = self.source.borrow_mut();
        if let Some(element) = self.current.take() {
            // TODO : Arbitrary constant.
            if borrow.1.len() < 16 {
                borrow.1.push_back(element);
            }
        }
        self.current = borrow.0.pop_front();
        &mut self.current
    }
}
