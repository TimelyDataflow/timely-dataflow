use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

pub struct SharedQueue<T> {
    queue: Arc<Mutex<VecDeque<T>>>
}

impl<T> SharedQueue<T> {
    pub fn push(&mut self, bytes: T) { self.queue.lock().expect("unable to lock shared queue").push_back(bytes) }
    pub fn pop(&mut self) -> Option<T> { self.queue.lock().expect("unable to lock shared queue").pop_front() }
    pub fn drain_into(&mut self, dest: &mut Vec<T>) { let mut lock = self.queue.lock().expect("unable to lock shared queue"); dest.extend(lock.drain(..)); }
    pub fn is_empty(&self) -> bool { self.queue.lock().expect("unable to lock shared queue").is_empty() }
    pub fn is_done(&self) -> bool { Arc::strong_count(&self.queue) == 1 }
    pub fn new() -> Self { SharedQueue { queue: Arc::new(Mutex::new(VecDeque::new())) } }
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        SharedQueue { queue: self.queue.clone() }
    }
}
