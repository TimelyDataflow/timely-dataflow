//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use allocator::{Allocate, AllocateBuilder, Message};
use {Push, Pull};


/// An allocator for intra-thread communication.
pub struct Thread;
impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self, _identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {
        let (pusher, puller) = Thread::new();
        (vec![Box::new(pusher)], Box::new(puller))
    }
}

impl AllocateBuilder for Thread {
    type Allocator = Self;
    fn build(self) -> Self { self }
}

impl Thread {
    /// Allocates a new pusher and puller pair.
    pub fn new<T: 'static>() -> (Pusher<T>, Puller<T>) {
        let shared = Rc::new(RefCell::new((VecDeque::<T>::new(), VecDeque::<T>::new())));
        (Pusher { target: shared.clone() }, Puller { source: shared, current: None })
    }
}


/// The push half of an intra-thread channel.
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

/// The pull half of an intra-thread channel.
pub struct Puller<T> {
    current: Option<T>,
    source: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<T> {
        let mut borrow = self.source.borrow_mut();
        // if let Some(element) = self.current.take() {
        //     // TODO : Arbitrary constant.
        //     if borrow.1.len() < 16 {
        //         borrow.1.push_back(element);
        //     }
        // }
        self.current = borrow.0.pop_front();
        &mut self.current
    }
}
