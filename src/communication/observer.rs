use std::mem;
use std::sync::mpsc::Sender;
use communication::Pushable;

// TODO : Using an Observer requires a &mut reference, and should have the "No races!" property:
// TODO : If you hold a &mut ref, no one else can call open/push/shut. Don't let go of that &mut!
// TODO : Probably a good place to insist on RAII... (see ObserverSession)

// observer trait
pub trait Observer {
    type Time;
    type Data;
    fn open(&mut self, time: &Self::Time);   // new punctuation, essentially ...
    fn push(&mut self, time: &Self::Data);   // reveals push data to the observer.
    fn shut(&mut self, time: &Self::Time);   // indicates that we are done for now.
}

// extension trait for creating an RAII observer session from any observer
pub trait ObserverSessionExt : Observer {
    fn session<'a>(&'a mut self, time: &'a <Self as Observer>::Time) -> ObserverSession<'a, Self>;
}

impl<O: Observer> ObserverSessionExt for O {
    #[inline(always)] fn session<'a>(&'a mut self, time: &'a <O as Observer>::Time) -> ObserverSession<'a, O> {
        ObserverSession::new(self, time)
    }
}

// Attempt at RAII for observers. Intended to prevent mis-sequencing of open/push/shut.
pub struct ObserverSession<'a, O:Observer+'a> where <O as Observer>::Time: 'a {
    observer:   &'a mut O,
    time:       &'a O::Time,
}

#[unsafe_destructor]
impl<'a, O:Observer+'a> Drop for ObserverSession<'a, O> where <O as Observer>::Time: 'a {
    #[inline(always)] fn drop(&mut self) { self.observer.shut(self.time); }
}

impl<'a, O:Observer+'a> ObserverSession<'a, O> where <O as Observer>::Time : 'a {
    #[inline(always)] fn new(obs: &'a mut O, time: &'a O::Time) -> ObserverSession<'a, O> {
        obs.open(time);
        ObserverSession { observer: obs, time: time }
    }
    #[inline(always)] pub fn push(&mut self, data: &O::Data) {
        self.observer.push(data);
    }
}

// implementation for inter-thread queues
impl<T:Clone+Send+'static, D:Clone+Send+'static> Observer for (Vec<D>, Sender<(T, Vec<D>)>) {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self,_time: &T) { }
    #[inline(always)] fn push(&mut self, data: &D) { self.0.push(data.clone()); }
    #[inline(always)] fn shut(&mut self, time: &T) { let vec = mem::replace(&mut self.0, Vec::new()); self.1.send((time.clone(), vec)).ok().expect("send error"); }
}


// an observer broadcasting to many observers
pub struct BroadcastObserver<O: Observer> {
    observers:  Vec<O>,
}

impl<O: Observer> Observer for BroadcastObserver<O> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { for observer in self.observers.iter_mut() { observer.open(time); } }
    #[inline(always)] fn push(&mut self, data: &O::Data) { for observer in self.observers.iter_mut() { observer.push(data); } }
    #[inline(always)] fn shut(&mut self, time: &O::Time) { for observer in self.observers.iter_mut() { observer.shut(time); } }
}


// an observer routing between many observers
pub struct ExchangeObserver<O: Observer, H: Fn(&O::Data) -> u64> {
    pub observers:  Vec<O>,
    pub hash_func:  H,
}

impl<O: Observer, H: Fn(&O::Data) -> u64+'static> Observer for ExchangeObserver<O, H> where O::Data : Clone {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () { for observer in self.observers.iter_mut() { observer.open(time); } }
    #[inline(always)] fn push(&mut self, data: &O::Data) -> () {
        let dst = (self.hash_func)(data) % self.observers.len() as u64;
        self.observers[dst as usize].push(data);
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () { for observer in self.observers.iter_mut() { observer.shut(time); } }
}

// an observer buffering records before sending
pub struct BufferedObserver<D, O: Observer> {
    limit:      u64,
    buffer:     Vec<D>,
    observer:   O,
}

impl<D: Clone+'static, O: Observer<Data = Vec<D>>> Observer for BufferedObserver<D, O> where O::Time : Clone + 'static {
    type Time = O::Time;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.observer.open(time); }
    #[inline(always)] fn push(&mut self, data: &D) -> () {
        self.buffer.push(data.clone());
        if self.buffer.len() as u64 > self.limit {
            self.observer.push(&mut self.buffer);
            self.buffer.clear();
        }
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        self.observer.push(&self.buffer);
        self.observer.shut(time);
        self.buffer.clear();
    }
}

// dual to BufferedObserver, flattens out buffers
pub struct FlattenedObserver<O: Observer> {
    observer:   O,
}

impl<O: Observer> Observer for FlattenedObserver<O> {
    type Time = O::Time;
    type Data = Vec<O::Data>;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () { self.observer.open(time); }
    #[inline(always)] fn push(&mut self, data: &Vec<O::Data>) -> () { for datum in data.iter() { self.observer.push(datum); } }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () { self.observer.shut(time); }
}


// discriminated union of two observers
pub enum ObserverPair<O1: Observer, O2: Observer> {
    Type1(O1),
    Type2(O2),
}

impl<T, D, O1: Observer<Time=T, Data=D>, O2: Observer<Time=T, Data=D>> Observer for ObserverPair<O1, O2> {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &T) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.open(time),
            ObserverPair::Type2(ref mut observer) => observer.open(time),
        }
    }
    #[inline(always)] fn push(&mut self, data: &D) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.push(data),
            ObserverPair::Type2(ref mut observer) => observer.push(data),
        }
    }
    #[inline(always)] fn shut(&mut self, time: &T) {
        match *self {
            ObserverPair::Type1(ref mut observer) => observer.shut(time),
            ObserverPair::Type2(ref mut observer) => observer.shut(time),
        }
    }
}
