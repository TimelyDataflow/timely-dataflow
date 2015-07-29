use std::rc::Rc;
use std::cell::RefCell;

pub use self::tee::{Tee, TeeHelper};
// pub use self::session::Session;
pub use self::exchange::Exchange;
pub use self::counter::Counter;

pub mod tee;
// pub mod session;
pub mod exchange;
pub mod counter;
pub mod buffer;

use communication::Message;

// TODO : Using an Observer requires a &mut reference, and should have the "No races!" property:
// TODO : If you hold a &mut ref, no one else can call open/push/shut. Don't let go of that &mut!
// TODO : Probably a good place to insist on RAII... (see ObserverSession)

// observer trait
pub trait Observer {
    type Time;
    type Data;
    fn open(&mut self, time: &Self::Time);
    fn shut(&mut self, time: &Self::Time);
    fn give(&mut self, data: &mut Message<Self::Data>);
}

pub struct BoxedObserver<T, D> {
    boxed: Box<ObserverBoxable<T, D>>,
}

impl<T, D> BoxedObserver<T, D> {
    pub fn new<O: Observer<Time=T, Data=D>+'static>(obs: O) -> BoxedObserver<T, D> {
        BoxedObserver {
            boxed: Box::new(obs),
        }
    }
}

impl<T, D> Observer for BoxedObserver<T, D> {
    type Time = T;
    type Data = D;
    fn open(&mut self, time: &Self::Time) { self.boxed.open_box(time); }
    fn shut(&mut self, time: &Self::Time) { self.boxed.shut_box(time); }
    fn give(&mut self, data: &mut Message<Self::Data>) {
        self.boxed.give_box(data);
    }
}

// observer trait. deals with ICE boxing Observers (traits with multiple associated types)
pub trait ObserverBoxable<T, D> {
    fn open_box(&mut self, time: &T);
    fn shut_box(&mut self, time: &T);
    fn give_box(&mut self, data: &mut Message<D>);
}

impl<O: Observer> ObserverBoxable<O::Time, O::Data> for O {
    fn open_box(&mut self, time: &O::Time) { self.open(time); }
    fn shut_box(&mut self, time: &O::Time) { self.shut(time); }
    fn give_box(&mut self, data: &mut Message<O::Data>) {
        self.give(data);
    }
}

// // extension trait for creating an RAII observer session from any observer
// pub trait Extensions : Observer {
//     fn session<'a>(&'a mut self, time: &Self::Time) -> session::Session<'a, Self>;
//     fn give_at<I: Iterator<Item=Self::Data>>(&mut self, time: &Self::Time, iter: I);
//     fn give_message_at(&mut self, time: &Self::Time, data: &mut Message<Self::Data>) {
//         self.open(time);
//         self.give(data);
//         self.shut(time);
//     }
// }

// impl<O: Observer> Extensions for O where O::Time: Clone {
//     fn session<'a>(&'a mut self, time: &Self::Time) -> session::Session<'a, Self> {
//         self.open(time);
//         session::Session::new(self, time)
//     }
//     fn give_at<I: Iterator<Item=O::Data>>(&mut self, time: &O::Time, iter: I) {
//         let mut session = self.session(time);
//         for item in iter { session.give(item); }
//     }
// }

// blanket implementation for Rc'd observers
impl<O: Observer> Observer for Rc<RefCell<O>> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) { self.borrow_mut().open(time); }
    #[inline] fn shut(&mut self, time: &O::Time) { self.borrow_mut().shut(time); }
    #[inline] fn give(&mut self, data: &mut Message<O::Data>) { self.borrow_mut().give(data); }
}

// blanket implementation for Box'd observers
impl<O: ?Sized + Observer> Observer for Box<O> {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) { (**self).open(time); }
    #[inline] fn shut(&mut self, time: &O::Time) { (**self).shut(time); }
    #[inline] fn give(&mut self, data: &mut Message<O::Data>) { (**self).give(data); }
}
