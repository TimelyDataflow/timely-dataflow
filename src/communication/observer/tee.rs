use std::rc::Rc;
use std::cell::RefCell;

// use communication::observer::ObserverBoxable;
// use communication::{Message, Observer};
// use serialization::Serializable;
use communication::message::Content;
use abomonation::Abomonation;

use fabric::Push;

// half of output_port for observing data
pub struct Tee<T, D> {
    buffer: Vec<D>,
    shared: Rc<RefCell<Vec<Box<Push<(T, Content<D>)>>>>>,
}

impl<T: Clone, D: Abomonation+Clone> Push<(T, Content<D>)> for Tee<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<(T, Content<D>)>) {
        if let Some((ref time, ref mut data)) = *message {
            let mut pushers = self.shared.borrow_mut();
            for index in (0..pushers.len()) {
                if index < pushers.len() - 1 {
                    // TODO : was `push_all`, but is now `extend`, slow.
                    self.buffer.extend(data.iter().cloned());
                    Content::push_at(&mut self.buffer, (*time).clone(), &mut pushers[index]);
                }
                else {
                    Content::push_at(data, (*time).clone(), &mut pushers[index]);
                    // pushers[index].push(data);
                }
            }
        }
        else {
            for pusher in self.shared.borrow_mut().iter_mut() {
                pusher.push(&mut None);
            }
        }
    }
}

impl<T, D> Tee<T, D> {
    pub fn new() -> (Tee<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = Tee {
            buffer: Vec::with_capacity(Content::<D>::default_length()),
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


// half of output_port used to add pushers
pub struct TeeHelper<T, D> {
    shared: Rc<RefCell<Vec<Box<Push<(T, Content<D>)>>>>>
}

impl<T, D> TeeHelper<T, D> {
    pub fn add_pusher<P: Push<(T, Content<D>)>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

// TODO : Implemented on behalf of example_static::Stream; check if truly needed.
impl<T, D> Clone for TeeHelper<T, D> {
    fn clone(&self) -> TeeHelper<T, D> { TeeHelper { shared: self.shared.clone() } }
}
