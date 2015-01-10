use std::mem;
use std::sync::mpsc::Sender;

// observer trait
pub trait Observer<T, D> : 'static {
    fn open(&mut self, time: &T);   // new punctuation, essentially ...
    fn push(&mut self, time: &D);   // reveals push data to the observer.
    fn shut(&mut self, time: &T);   // indicates that we are done for now.
}

// implementation for inter-thread queues
impl<T:Clone+Send+'static, D:Clone+Send+'static> Observer<T, D> for (Vec<D>, Sender<(T, Vec<D>)>) {
    fn open(&mut self, time: &T) { }
    fn push(&mut self, data: &D) { self.0.push(data.clone()); }
    fn shut(&mut self, time: &T) { let vec = mem::replace(&mut self.0, Vec::new()); self.1.send((time.clone(), vec)); }
}

// an observer broadcasting to many observers
pub struct BroadcastObserver<T, D, O: Observer<T, D>> {
    observers:  Vec<O>,
}

impl<T: Clone, D: Clone, O: Observer<T, D>> Observer<T, D> for BroadcastObserver<T, D, O> {
    fn open(&mut self, time: &T) { for observer in self.observers.iter_mut() { observer.open(time); } }
    fn push(&mut self, data: &D) { for observer in self.observers.iter_mut() { observer.push(data); } }
    fn shut(&mut self, time: &T) { for observer in self.observers.iter_mut() { observer.shut(time); }}
}


// an observer routing between many observers
pub struct ExchangeObserver<T, D, O: Observer<T, D>, H: Fn(&D) -> u64> {
    pub observers:  Vec<O>,
    pub hash_func:  H,
}

impl<T, D: Clone, O: Observer<T, D>, H: Fn(&D) -> u64+'static> Observer<T, D> for ExchangeObserver<T, D, O, H> {
    fn open(&mut self, time: &T) -> () { for observer in self.observers.iter_mut() { observer.open(time); } }
    fn push(&mut self, data: &D) -> () {
        let dst = (self.hash_func)(data) % self.observers.len() as u64;
        self.observers[dst as usize].push(data);
    }
    fn shut(&mut self, time: &T) -> () { for observer in self.observers.iter_mut() { observer.shut(time); } }
}

// an observer buffering records before sending
pub struct BufferedObserver<T, D, O: Observer<T, Vec<D>>> {
    limit:      u64,
    buffer:     Vec<D>,
    observer:   O,
}

impl<T:Clone+'static, D: Clone+'static, O: Observer<T, Vec<D>>> Observer<T, D> for BufferedObserver<T, D, O> {
    fn open(&mut self, time: &T) { self.observer.open(time); }
    fn push(&mut self, data: &D) -> () {
        self.buffer.push(data.clone());
        if self.buffer.len() as u64 > self.limit {
            self.observer.push(&mut self.buffer);
            self.buffer.clear();
        }
    }
    fn shut(&mut self, time: &T) -> () {
        self.observer.push(&self.buffer);
        self.observer.shut(time);
        self.buffer.clear();
    }
}

// dual to BufferedObserver, flattens out buffers
pub struct FlattenedObserver<T, D, O: Observer<T, D>> {
    observer:   O,
}

impl<T, D, O: Observer<T, D>> Observer<T, Vec<D>> for FlattenedObserver<T, D, O> {
    fn open(&mut self, time: &T) -> () { self.observer.open(time); }
    fn push(&mut self, data: &Vec<D>) -> () { for datum in data.iter() { self.observer.push(datum); } }
    fn shut(&mut self, time: &T) -> () { self.observer.shut(time); }
}


// discriminated union of two observers
pub enum ObserverPair<T, D, O1: Observer<T, D>, O2: Observer<T, D>> {
    Type1(O1),
    Type2(O2),
}

impl<T, D, O1: Observer<T, D>, O2: Observer<T, D>> Observer<T, D> for ObserverPair<T, D, O1, O2> {
    fn open(&mut self, time: &T) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.open(time),
            ObserverPair::Type2(ref mut observer) => observer.open(time),
        }
    }
    fn push(&mut self, data: &D) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.push(data),
            ObserverPair::Type2(ref mut observer) => observer.push(data),
        }
    }
    fn shut(&mut self, time: &T) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.shut(time),
            ObserverPair::Type2(ref mut observer) => observer.shut(time),
        }
    }
}
