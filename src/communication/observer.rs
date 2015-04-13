use std::rc::Rc;
use std::cell::RefCell;

// TODO : Using an Observer requires a &mut reference, and should have the "No races!" property:
// TODO : If you hold a &mut ref, no one else can call open/push/shut. Don't let go of that &mut!
// TODO : Probably a good place to insist on RAII... (see ObserverSession)

// observer trait
pub trait Observer {
    type Time;
    type Data;
    fn open(&mut self, time: &Self::Time);   // new punctuation, essentially ...
    fn show(&mut self, data: &Self::Data);   // reveals data to the observer.
    fn give(&mut self, data:  Self::Data);   // gives data to the observer.
    fn shut(&mut self, time: &Self::Time);   // indicates that we are done for now.
}

// extension trait for creating an RAII observer session from any observer
pub trait ObserverSessionExt : Observer {
    fn session<'a>(&'a mut self, time: &'a <Self as Observer>::Time) -> ObserverSession<'a, Self>;
    // TODO : Rust doesn't like this; "can't find push()", so probably some type constraints not met?
    // TODO : All those lifetime bounds are just things I put in to help, not actually clearly important.
    // fn send_at<'a, I: Iterator<Item=<Self as Observer>::Data>>(&'a mut self, time: &'a <Self as Observer>::Time, iter: I) where Self: 'a {
    //     let session = self.session(time);
    //     for item in iter {
    //         session.push(&item);
    //     }
    // }
}

impl<O: Observer> ObserverSessionExt for O {
    #[inline(always)] fn session<'a>(&'a mut self, time: &'a <O as Observer>::Time) -> ObserverSession<'a, O> {
        self.open(time);
        ObserverSession { observer: self, time: time }
    }
}

// Attempt at RAII for observers. Intended to prevent mis-sequencing of open/push/shut.
pub struct ObserverSession<'a, O:Observer+'a> where <O as Observer>::Time: 'a {
    observer:   &'a mut O,
    time:       &'a O::Time,
}

impl<'a, O:Observer+'a> Drop for ObserverSession<'a, O> where <O as Observer>::Time: 'a {
    #[inline(always)] fn drop(&mut self) { self.observer.shut(self.time); }
}

impl<'a, O:Observer+'a> ObserverSession<'a, O> where <O as Observer>::Time : 'a {
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
// an observer broadcasting to many observers
pub struct BroadcastObserver<O: Observer> {
    observers:  Vec<O>,
}

impl<O: Observer> BroadcastObserver<O> {
    pub fn new() -> BroadcastObserver<O> { BroadcastObserver { observers: Vec::new() }}
    pub fn add(&mut self, observer: O) { self.observers.push(observer); }
}

impl<O: Observer> Observer for BroadcastObserver<O> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { for observer in self.observers.iter_mut() { observer.open(time); } }
    #[inline(always)] fn show(&mut self, data: &O::Data) { for observer in self.observers.iter_mut() { observer.show(data); } }
    #[inline(always)] fn give(&mut self, data:  O::Data) {
        // TODO : Hand ownership to the last observer
        for index in (0..self.observers.len() - 1) { self.observers[index].show(&data); }
        if self.observers.len() > 0 {
            let last = self.observers.len() - 1;
            self.observers[last].give(data);
        }
        // for observer in self.observers.iter_mut() { observer.show(&data); }
    }
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

// an observer buffering records before sending
pub struct BufferedObserver<D, O: Observer> {
    limit:      u64,
    buffer:     Vec<D>,
    observer:   O,
}

impl<D, O: Observer<Data = Vec<D>>> BufferedObserver<D, O> {
    pub fn inner(&self) -> &O { &self.observer }
    pub fn inner_mut(&mut self) -> &mut O { &mut self.observer }
    pub fn new(limit: u64, observer: O) -> BufferedObserver<D, O> {
        BufferedObserver {
            limit: limit,
            buffer: Vec::with_capacity(limit as usize),
            observer: observer,
        }
    }
}

// TODO : These clones are really expensive. Hard to avoid, because we can't stash references.
impl<D: Clone+'static, O: Observer<Data = Vec<D>>> Observer for BufferedObserver<D, O> {
    type Time = O::Time;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.observer.open(time); }
    #[inline(always)] fn show(&mut self, data: &D) { self.give(data.clone()); }
    #[inline(always)] fn give(&mut self, data:  D) {
        self.buffer.push(data);
        if self.buffer.len() as u64 > self.limit {
            self.observer.show(&mut self.buffer);
            self.buffer.clear();
        }
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) {
        if self.buffer.len() > 0 {
            self.observer.show(&self.buffer);
            self.buffer.clear();
        }
        self.observer.shut(time);
    }
}

// dual to BufferedObserver, flattens out buffers
pub struct FlattenedObserver<O: Observer> {
    observer:   O,
}

impl<O: Observer> FlattenedObserver<O> {
    pub fn new(observer: O) -> FlattenedObserver<O> { FlattenedObserver { observer: observer }}
}

impl<O: Observer> Observer for FlattenedObserver<O> {
    type Time = O::Time;
    type Data = Vec<O::Data>;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () { self.observer.open(time); }
    #[inline(always)] fn show(&mut self, data: &Vec<O::Data>) -> () { for datum in data { self.observer.show(datum); } }
    #[inline(always)] fn give(&mut self, data:  Vec<O::Data>) -> () { for datum in data { self.observer.give(datum); } }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () { self.observer.shut(time); }
}


// // discriminated union of two observers
// pub enum ObserverPair<O1: Observer, O2: Observer> {
//     Type1(O1),
//     Type2(O2),
// }
//
// impl<T, D, O1: Observer<Time=T, Data=D>, O2: Observer<Time=T, Data=D>> Observer for ObserverPair<O1, O2> {
//     type Time = T;
//     type Data = D;
//     #[inline(always)]
//     fn open(&mut self, time: &T) {
//         match *self {
//             ObserverPair::Type1(ref mut observer) => observer.open(time),
//             ObserverPair::Type2(ref mut observer) => observer.open(time),
//         }
//     }
//     #[inline(always)]
//     fn push(&mut self, data: &D) {
//         match *self {
//             ObserverPair::Type1(ref mut observer) => observer.push(data),
//             ObserverPair::Type2(ref mut observer) => observer.push(data),
//         }
//     }
//     #[inline(always)]
//     fn shut(&mut self, time: &T) {
//         match *self {
//             ObserverPair::Type1(ref mut observer) => observer.shut(time),
//             ObserverPair::Type2(ref mut observer) => observer.shut(time),
//         }
//     }
// }
