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
    fn session<'a>(&'a mut self, time: &Self::Time) -> ObserverSession<'a, Self>;
    fn give_at<I: Iterator<Item=Self::Data>>(&mut self, time: &Self::Time, iter: I);
    fn give_vector_at(&mut self, time: &Self::Time, data: &mut Vec<Self::Data>);
}

impl<O: Observer> ObserverSessionExt for O where O::Time: Clone {
    fn session<'a>(&'a mut self, time: &O::Time) -> ObserverSession<'a, O> {
        self.open(time);
        ObserverSession { buffer: Vec::with_capacity(8192), observer: self, time: (*time).clone() }
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
pub struct ObserverSession<'a, O:Observer+'a> {
    buffer:     Vec<O::Data>,
    observer:   &'a mut O,
    time:       O::Time,
}

impl<'a, O:Observer> Drop for ObserverSession<'a, O> {
    #[inline] fn drop(&mut self) {
        if self.buffer.len() > 0 {
            self.observer.give(&mut self.buffer);
        }
        self.observer.shut(&self.time);
    }
}

impl<'a, O:Observer> ObserverSession<'a, O> {
    #[inline(always)] pub fn give(&mut self, data:  O::Data) {
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            self.observer.give(&mut self.buffer);
            self.buffer.clear();
        }
    }
}


// blanket implementation for Rc'd observers
impl<O: Observer> Observer for Rc<RefCell<O>> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) { self.borrow_mut().open(time); }
    #[inline] fn shut(&mut self, time: &O::Time) { self.borrow_mut().shut(time); }
    #[inline] fn give(&mut self, data: &mut Vec<O::Data>) { self.borrow_mut().give(data); }
}

// blanket implementation for Box'd observers
impl<O: ?Sized + Observer> Observer for Box<O> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) { (**self).open(time); }
    #[inline] fn shut(&mut self, time: &O::Time) { (**self).shut(time); }
    #[inline] fn give(&mut self, data: &mut Vec<O::Data>) { (**self).give(data); }
}

// an observer routing between multiple observers
// TODO : Software write combining
pub struct ExchangeObserver<O: Observer, H: Fn(&O::Data) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
    buffers:    Vec<Vec<O::Data>>,
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver<O, H> {
    pub fn new(obs: Vec<O>, key: H) -> ExchangeObserver<O, H> {
        let mut buffers = vec![]; for _ in 0..obs.len() { buffers.push(Vec::with_capacity(8192)); }
        ExchangeObserver {
            observers: obs,
            hash_func: key,
            buffers: buffers,
        }
    }
}
impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for ExchangeObserver<O, H> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) -> () {
        for observer in self.observers.iter_mut() { observer.open(time); }
    }
    #[inline] fn shut(&mut self, time: &O::Time) -> () {
        for (index, mut buffer) in self.buffers.iter_mut().enumerate() {
            if buffer.len() > 0 {
                self.observers[index].give(&mut buffer);
                buffer.clear();
            }
        }

        for observer in self.observers.iter_mut() { observer.shut(time); }
    }
    #[inline] fn give(&mut self, data: &mut Vec<O::Data>) {
        // cheeky optimization for single thread
        if self.observers.len() == 1 {
            self.observers[0].give(data);
        }
        // if the number of observers is a power of two, use a mask
        else if (self.observers.len() & (self.observers.len() - 1)) == 0 {
            let mask = (self.observers.len() - 1) as u64;
            for datum in data.drain_temp() {
                let index = (((self.hash_func)(&datum)) & mask) as usize;
                self.buffers[index].push(datum);
                if self.buffers[index].len() == self.buffers[index].capacity() {
                    self.observers[index].give(&mut self.buffers[index]);
                    self.buffers[index].clear();
                }
            }
        }
        // as a last resort, use mod (%)
        else {
            for datum in data.drain_temp() {
                let index = (((self.hash_func)(&datum)) % self.observers.len() as u64) as usize;
                self.buffers[index].push(datum);
                if self.buffers[index].len() == self.buffers[index].capacity() {
                    self.observers[index].give(&mut self.buffers[index]);
                    self.buffers[index].clear();
                }
            }
        }
    }
}
