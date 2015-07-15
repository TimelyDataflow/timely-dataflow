use std::rc::Rc;
use std::cell::RefCell;
use std::mem;
use std::default::Default;

// TODO : Using an Observer requires a &mut reference, and should have the "No races!" property:
// TODO : If you hold a &mut ref, no one else can call open/push/shut. Don't let go of that &mut!
// TODO : Probably a good place to insist on RAII... (see ObserverSession)

// observer trait
pub trait Observer {
    type Time;
    type Data;
    fn open(&mut self, time: &Self::Time);   // new punctuation, essentially ...
    fn show(&mut self, data: &Self::Data);   // shows data to the observer.
    fn give(&mut self, data:  Self::Data);   // gives data to the observer.
    fn shut(&mut self, time: &Self::Time);   // indicates that we are done for now.
}

// extension trait for creating an RAII observer session from any observer
pub trait ObserverSessionExt : Observer {
    fn session<'a>(&'a mut self, time: &'a Self::Time) -> ObserverSession<'a, Self>;
    fn show_at<'a, I: Iterator<Item=&'a Self::Data>>(&mut self, time: &Self::Time, iter: I) where Self::Data: 'a;
    fn give_at<I: Iterator<Item=Self::Data>>(&mut self, time: &Self::Time, iter: I);
}

impl<O: Observer> ObserverSessionExt for O {
    #[inline(always)] fn session<'a>(&'a mut self, time: &'a O::Time) -> ObserverSession<'a, O> {
        self.open(time);
        ObserverSession { observer: self, time: time }
    }
    fn show_at<'a, I: Iterator<Item=&'a O::Data>>(&mut self, time: &O::Time, iter: I) where O::Data: 'a {
        self.open(time);
        for item in iter { self.show(item); }
        self.shut(time);
    }
    fn give_at<I: Iterator<Item=O::Data>>(&mut self, time: &O::Time, iter: I) {
        self.open(time);
        for item in iter { self.give(item); }
        self.shut(time);
    }
}

// Attempt at RAII for observers. Intended to prevent mis-sequencing of open/push/shut.
pub struct ObserverSession<'a, O:Observer+'a> where O::Time: 'a {
    observer:   &'a mut O,
    time:       &'a O::Time,
}

impl<'a, O:Observer> Drop for ObserverSession<'a, O> where O::Time: 'a {
    #[inline(always)] fn drop(&mut self) { self.observer.shut(self.time); }
}

impl<'a, O:Observer> ObserverSession<'a, O> where O::Time: 'a {
    #[inline(always)] pub fn show(&mut self, data: &O::Data) { self.observer.show(data); }
    #[inline(always)] pub fn give(&mut self, data:  O::Data) { self.observer.give(data); }
}


// blanket implementation for Rc'd observers
impl<O: Observer> Observer for Rc<RefCell<O>> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.borrow_mut().open(time); }
    #[inline(always)] fn show(&mut self, data: &O::Data) { self.borrow_mut().show(data); }
    #[inline(always)] fn give(&mut self, data:  O::Data) { self.borrow_mut().give(data); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) { self.borrow_mut().shut(time); }
}

// blanket implementation for Box'd observers
impl<O: ?Sized + Observer> Observer for Box<O> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { (**self).open(time); }
    #[inline(always)] fn show(&mut self, data: &O::Data) { (**self).show(data); }
    #[inline(always)] fn give(&mut self, data:  O::Data) { (**self).give(data); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) { (**self).shut(time); }
}

// an observer routing between many observers
pub struct ExchangeObserver<O: Observer, H: Fn(&O::Data) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver<O, H> {
    pub fn new(obs: Vec<O>, key: H) -> ExchangeObserver<O, H> {
        ExchangeObserver {
            observers: obs,
            hash_func: key,
        }
    }
}

impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for ExchangeObserver<O, H> where O::Data : Clone {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () { for observer in self.observers.iter_mut() { observer.open(time); } }
    #[inline(always)] fn show(&mut self, data: &O::Data) -> () {
        let dst = (self.hash_func)(data) % self.observers.len() as u64;
        self.observers[dst as usize].show(data);
    }
    #[inline(always)] fn give(&mut self, data:  O::Data) -> () {
        let dst = (self.hash_func)(&data) % self.observers.len() as u64;
        self.observers[dst as usize].give(data);
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () { for observer in self.observers.iter_mut() { observer.shut(time); } }
}

// an observer routing between 256 observers
pub struct ExchangeObserver256<O: Observer, H: Fn(&O::Data) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
    buffer:     Vec<O::Data>,
    counts:     Vec<u8>,
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver256<O, H> where O::Data: Clone+Default {
    pub fn new(obs: Vec<O>, key: H) -> ExchangeObserver256<O, H> {
        ExchangeObserver256 {
            observers: obs,
            hash_func: key,
            buffer: vec![Default::default(); 256 * 8],  // should use mem::size_of::<O::Data>()
            counts: vec![0; 256],
        }
    }
}
impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for ExchangeObserver256<O, H> where O::Data : Clone+Default {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () {
        for observer in self.observers.iter_mut() { observer.open(time); }
    }
    #[inline(always)] fn show(&mut self, data: &O::Data) -> () {
        self.give((*data).clone());
    }
    #[inline(always)] fn give(&mut self, data:  O::Data) -> () {
        self.push_to_buffer(data);
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        for index in 0..256 {
            if self.counts[index] > 0 {
                self.flush_buffer(index);
            }
        }

        for observer in self.observers.iter_mut() { observer.shut(time); }
    }
}

impl<O: Observer, H: Fn(&O::Data) -> u64> ExchangeObserver256<O, H> where O::Data : Clone+Default {
    fn push_to_buffer(&mut self, data: O::Data) {
        let byte = (((self.hash_func)(&data)) % 256) as usize;
        mem::replace(&mut self.buffer[8 * byte + self.counts[byte] as usize], data);
        self.counts[byte] += 1;
        if self.counts[byte] == 8 {
            self.flush_buffer(byte);
        }
    }
    fn flush_buffer(&mut self, buffer_index: usize) {
        let dest_index = buffer_index % self.observers.len();
        let dest = &mut self.observers[dest_index];
        for index in 0..self.counts[buffer_index] as usize {
            dest.give(mem::replace(&mut self.buffer[8 * buffer_index + index], Default::default()));
        }
        self.counts[buffer_index] = 0;
    }
}
