use std::mem;
use std::sync::mpsc::{Sender, Receiver};

// observer trait
pub trait Observer<T> : 'static {
    fn next(&mut self, data: T); // reveals more data to the observer.
    fn done(&mut self);          // indicates that we are done for now.
}


// implementation for inter-thread queues
impl<T:Send+'static> Observer<T> for Sender<T> {
    fn next(&mut self, data: T) { self.send(data); }
    fn done(&mut self) { }
}

// an observer broadcasting to many observers
pub struct BroadcastObserver<T, O: Observer<T>> {
    observers:  Vec<O>,
}

impl<T: Clone, O: Observer<T>> Observer<T> for BroadcastObserver<T, O> {
    #[inline(always)]
    fn next(&mut self, record: T) {
        for observer in self.observers.iter_mut() { observer.next(record.clone()); }
    }

    fn done(&mut self) { for observer in self.observers.iter_mut() { observer.done(); }}
}


// an observer routing between many observers
pub struct ExchangeObserver<T, O: Observer<T>, H: Fn(&T) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
}

impl<T, O: Observer<T>, H: Fn(&T) -> u64+'static> Observer<T> for ExchangeObserver<T, O, H> {
    #[inline(always)]
    fn next(&mut self, record: T) -> () {
        let len = self.observers.len() as u64;
        self.observers[((self.hash_func)(&record) % len) as uint].next(record);
    }

    fn done(&mut self) -> () { for observer in self.observers.iter_mut() { observer.done(); } }
}

// an observer buffering records before sending
pub struct BufferedObserver<T, O: Observer<Vec<T>>> {
    limit:      u64,
    buffer:     Vec<T>,
    observer:   O,
}

impl<T:'static, O: Observer<Vec<T>>> Observer<T> for BufferedObserver<T, O> {
    #[inline(always)]
    fn next(&mut self, record: T) -> () {
        self.buffer.push(record);
        if self.buffer.len() as u64 > self.limit {
            self.observer.next(mem::replace(&mut self.buffer, Vec::new()));
        }
    }

    fn done(&mut self) -> () {
        self.observer.next(mem::replace(&mut self.buffer, Vec::new()));
        self.observer.done();
    }
}

// dual to BufferedObserver, flattens out buffers
pub struct FlattenedObserver<T, O: Observer<T>> {
    observer:   O,
}

impl<T, O: Observer<T>> Observer<Vec<T>> for FlattenedObserver<T, O> {
    #[inline(always)]
    fn next(&mut self, records: Vec<T>) -> () {
        for record in records.into_iter() { self.observer.next(record); }
    }

    fn done(&mut self) -> () { self.observer.done(); }
}


// discriminated union of two observers
pub enum ObserverPair<T, O1: Observer<T>, O2: Observer<T>> {
    Type1(O1),
    Type2(O2),
}

impl<T, O1: Observer<T>, O2: Observer<T>> Observer<T> for ObserverPair<T, O1, O2> {
    #[inline(always)]
    fn next(&mut self, data: T) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.next(data),
            ObserverPair::Type2(ref mut observer) => observer.next(data),
        }
    }

    fn done(&mut self) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.done(),
            ObserverPair::Type2(ref mut observer) => observer.done(),
        }
    }
}
