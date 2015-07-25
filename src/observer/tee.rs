use std::rc::Rc;
use std::cell::RefCell;

use observer::{Observer, ObserverBoxable};
use communicator::Message;
use serialization::Serializable;

// half of output_port for observing data
pub struct Tee<T, D> {
    buffer: Vec<D>,
    shared: Rc<RefCell<Vec<Box<ObserverBoxable<T, D>>>>>,
}

impl<T, D: Clone+Serializable> Observer for Tee<T, D> {
    type Time = T;
    type Data = D;

    #[inline]
    fn open(&mut self, time: &T) {
        for observer in self.shared.borrow_mut().iter_mut() { observer.open_box(time); }
    }

    #[inline] fn shut(&mut self, time: &T) {
        for observer in self.shared.borrow_mut().iter_mut() { observer.shut_box(time); }
    }
    #[inline] fn give(&mut self, data: &mut Message<D>) {
        let mut observers = self.shared.borrow_mut();
        for index in (0..observers.len()) {
            if index < observers.len() - 1 {
                // TODO : was push_all, but is now extend.
                // TODO : currently extend is slow. watch.
                self.buffer.extend(data.look().iter().cloned());
                let mut message = Message::Typed(::std::mem::replace(&mut self.buffer, Vec::new()));
                observers[index].give_box(&mut message);
                self.buffer = if let Message::Typed(mut buffer) = message { buffer.clear(); buffer }
                              else { Vec::with_capacity(4096) };
            }
            else {
                observers[index].give_box(data);
            }
        }
        // data.clear();
    }
}

impl<T, D> Tee<T, D> {
    pub fn new() -> (Tee<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = Tee {
            buffer: Vec::new(),
            shared: shared.clone(),
        };

        (port, TeeHelper { shared: shared })
    }
}

impl<T, D> Clone for Tee<T, D> {
    fn clone(&self) -> Tee<T, D> {
        Tee {
            buffer: Vec::with_capacity(self.buffer.capacity()),
            shared: self.shared.clone(),
        }
    }
}


// half of output_port used to add observers
pub struct TeeHelper<T, D> {
    shared: Rc<RefCell<Vec<Box<ObserverBoxable<T, D>>>>>
}

impl<T, D> TeeHelper<T, D> {
    pub fn add_observer<O: Observer<Time=T, Data=D>+'static>(&self, observer: O) {
        self.shared.borrow_mut().push(Box::new(observer));
    }
}

// TODO : Implemented on behalf of example_static::Stream; check if truly needed.
impl<T, D> Clone for TeeHelper<T, D> {
    fn clone(&self) -> TeeHelper<T, D> { TeeHelper { shared: self.shared.clone() } }
}
