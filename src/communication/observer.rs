use std::rc::Rc;
use std::cell::RefCell;
use drain::DrainExt;

// TODO : Using an Observer requires a &mut reference, and should have the "No races!" property:
// TODO : If you hold a &mut ref, no one else can call open/push/shut. Don't let go of that &mut!
// TODO : Probably a good place to insist on RAII... (see ObserverSession)

// observer trait
pub trait Observer {
    type Time;
    type Data;
    fn open(&mut self, time: &Self::Time);   // new punctuation, essentially ...
    fn shut(&mut self, time: &Self::Time);   // indicates that we are done for now.
    fn give(&mut self, data: &mut Vec<Self::Data>);
}

// extension trait for creating an RAII observer session from any observer
pub trait ObserverSessionExt : Observer {
    fn session<'a>(&'a mut self, time: &'a Self::Time) -> ObserverSession<'a, Self>;
    fn give_at<I: Iterator<Item=Self::Data>>(&mut self, time: &Self::Time, iter: I);
    fn give_vector_at(&mut self, time: &Self::Time, data: &mut Vec<Self::Data>);
}

impl<O: Observer> ObserverSessionExt for O {
    fn session<'a>(&'a mut self, time: &'a O::Time) -> ObserverSession<'a, O> {
        self.open(time);
        ObserverSession { buffer: Vec::with_capacity(256), observer: self, time: time }
    }
    fn give_at<I: Iterator<Item=O::Data>>(&mut self, time: &O::Time, iter: I) {
        let mut session = self.session(time);
        for item in iter { session.give(item); }
    }
    fn give_vector_at(&mut self, time: &O::Time, data: &mut Vec<O::Data>) {
        self.open(time);
        self.give(data);
        self.shut(time);
    }
}

// Attempt at RAII for observers. Intended to prevent mis-sequencing of open/push/shut.
pub struct ObserverSession<'a, O:Observer+'a> where O::Time: 'a {
    buffer:     Vec<O::Data>,
    observer:   &'a mut O,
    time:       &'a O::Time,
}

impl<'a, O:Observer> Drop for ObserverSession<'a, O> where O::Time: 'a {
    #[inline(always)] fn drop(&mut self) {
        if self.buffer.len() > 0 {
            self.observer.give(&mut self.buffer);
        }
        self.observer.shut(self.time);
    }
}

impl<'a, O:Observer> ObserverSession<'a, O> where O::Time: 'a {
    #[inline(always)] pub fn give(&mut self, data:  O::Data) {
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            self.observer.give(&mut self.buffer);
        }
    }
}


// blanket implementation for Rc'd observers
impl<O: Observer> Observer for Rc<RefCell<O>> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.borrow_mut().open(time); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) { self.borrow_mut().shut(time); }
    #[inline(always)] fn give(&mut self, data:  &mut Vec<O::Data>) { self.borrow_mut().give(data); }
}

// blanket implementation for Box'd observers
impl<O: ?Sized + Observer> Observer for Box<O> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { (**self).open(time); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) { (**self).shut(time); }
    #[inline(always)] fn give(&mut self, data: &mut Vec<O::Data>) { (**self).give(data); }
}

// an observer routing between 256 observers
// TODO : Software write combining
pub struct ExchangeObserver<O: Observer, H: Fn(&O::Data) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
    buffers:    Vec<Vec<O::Data>>,
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver<O, H> {
    pub fn new(obs: Vec<O>, key: H) -> ExchangeObserver<O, H> {
        let mut buffers = vec![]; for _ in 0..obs.len() { buffers.push(Vec::with_capacity(256)); }
        ExchangeObserver {
            observers: obs,
            hash_func: key,
            buffers: buffers,
        }
    }
}
impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for ExchangeObserver<O, H> where O::Data : Clone {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () {
        for observer in self.observers.iter_mut() { observer.open(time); }
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        for index in 0..self.observers.len() {
            if self.buffers[index].len() > 0 {
                self.observers[index].give(&mut self.buffers[index]);
            }
        }

        for observer in self.observers.iter_mut() { observer.shut(time); }
    }
    #[inline(always)] fn give(&mut self, data: &mut Vec<O::Data>) {
        for datum in data.drain_temp() {
            self.push_to_buffer(datum);
        }
    }
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver<O, H> {
    fn push_to_buffer(&mut self, data: O::Data) {
        let index = (((self.hash_func)(&data)) % self.observers.len() as u64) as usize;
        self.buffers[index].push(data);
        if self.buffers[index].len() >= 256 {
            self.observers[index].give(&mut self.buffers[index]);
        }
    }
}


// an observer routing between 256 observers
// TODO : Software write combining
pub struct ExchangeObserver256<O: Observer, H: Fn(&O::Data) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
    buffers:    Vec<Vec<O::Data>>,
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver256<O, H> {
    pub fn new(obs: Vec<O>, key: H) -> ExchangeObserver256<O, H> {
        let mut buffers = vec![]; for _ in 0..256 { buffers.push(Vec::with_capacity(256)); }
        ExchangeObserver256 {
            observers: obs,
            hash_func: key,
            buffers: buffers,
        }
    }
}
impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for ExchangeObserver256<O, H> where O::Data : Clone {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () {
        for observer in self.observers.iter_mut() { observer.open(time); }
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        let observers = self.observers.len();
        for index in 0..256 {
            if self.buffers[index].len() > 0 {
                self.observers[index % observers].give(&mut self.buffers[index]);
            }
        }

        for observer in self.observers.iter_mut() { observer.shut(time); }
    }
    #[inline(always)] fn give(&mut self, data: &mut Vec<O::Data>) {
        for datum in data.drain_temp() {
            self.push_to_buffer(datum);
        }
    }
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver256<O, H> {
    fn push_to_buffer(&mut self, data: O::Data) {
        let byte = (((self.hash_func)(&data)) % 256) as usize;
        self.buffers[byte].push(data);
        if self.buffers[byte].len() >= 256 {
            let observers = self.observers.len();
            self.observers[byte % observers].give(&mut self.buffers[byte]);
        }
    }
}
