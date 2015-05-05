use std::rc::Rc;
use std::cell::RefCell;
use std::sync::mpsc::{Sender, Receiver};

use std::collections::VecDeque;

pub trait Pushable<T> { fn push(&mut self, data: T); }        // like observer
pub trait Pullable<T> { fn pull(&mut self) -> Option<T>; }    // like iterator

// impl<T> Pushable<T> for Vec<T> { fn push(&mut self, data: T) { self.push(data); } }
// impl<T> Pullable<T> for Vec<T> { fn pull(&mut self) -> Option<T> { self.pop() } }

impl<T> Pushable<T> for VecDeque<T> { fn push(&mut self, data: T) { self.push_back(data); } }
impl<T> Pullable<T> for VecDeque<T> { fn pull(&mut self) -> Option<T> { self.pop_front() } }


impl<T:Send> Pushable<T> for Sender<T> { fn push(&mut self, data: T) { self.send(data).ok().expect("send error"); } }
impl<T:Send> Pullable<T> for Receiver<T> { fn pull(&mut self) -> Option<T> { self.try_recv().ok() }}

impl<T, P: ?Sized + Pushable<T>> Pushable<T> for Box<P> { fn push(&mut self, data: T) { (**self).push(data); } }
impl<T, P: ?Sized + Pullable<T>> Pullable<T> for Box<P> { fn pull(&mut self) -> Option<T> { (**self).pull() } }

impl<T, P: Pushable<T>> Pushable<T> for Rc<RefCell<P>> { fn push(&mut self, data: T) { self.borrow_mut().push(data); } }
impl<T, P: Pullable<T>> Pullable<T> for Rc<RefCell<P>> { fn pull(&mut self) -> Option<T> { self.borrow_mut().pull() } }
