use std::sync::mpsc::{Sender, Receiver};

// pub struct SharedQueue<T> {
//     queue: Arc<(Mutex<VecDeque<T>>, CondVar)>,
// }

// impl<T> SharedQueue<T> {
//     pub fn push(&mut self, item: T) {
//         let lock = queue.0.lock();
//         lock.push_back(item);
//         queue.1.notify_all();
//     }

//     pub fn pop(&mut self) -> Option<T> {

//     }
// }

pub struct SharedQueueSend<T> {
    queue: Sender<T>,
}

impl<T> SharedQueueSend<T> {
    pub fn push(&mut self, item: T) {
        self.queue
            .send(item)
            .expect("unable to lock shared queue");
    }
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }
    pub fn from(queue: Sender<T>) -> Self {
        SharedQueueSend {
            queue,
        }
    }
}

pub struct SharedQueueRecv<T> {
    queue: Receiver<T>,
}

impl<T> SharedQueueRecv<T> {
    pub fn pop(&mut self) -> Option<T> {
        self.queue.try_recv().ok()
    }
    pub fn from(queue: Receiver<T>) -> Self { SharedQueueRecv { queue } }
}