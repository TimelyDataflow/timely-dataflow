use std::sync::{Arc, mpsc::{Sender, Receiver}};

pub struct SharedQueueSend<T> {
    queue: Sender<(T, Arc<()>)>,
    count: Arc<()>,
}

impl<T> SharedQueueSend<T> {
    pub fn push(&mut self, item: T) {
        self.queue
            .send((item, self.count.clone()))
            .expect("unable to lock shared queue");
    }
    pub fn is_empty(&self) -> bool {
        Arc::strong_count(&self.count) == 1
    }
    pub fn from(queue: Sender<(T, Arc<()>)>) -> Self {
        SharedQueueSend {
            queue,
            count: Arc::new(()),
        }
    }
}

pub struct SharedQueueRecv<T> {
    queue: Receiver<(T, Arc<()>)>,
}

impl<T> SharedQueueRecv<T> {
    pub fn pop(&mut self) -> Option<T> {
        self.queue.try_recv().ok().map(|(item, _count)| item)
    }
    pub fn from(queue: Receiver<(T, Arc<()>)>) -> Self { SharedQueueRecv { queue } }
}