//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::rc::Rc;
use std::cell::RefCell;

use dataflow::channels::Content;
use abomonation::Abomonation;

use timely_communication::Push;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct Tee<T: 'static, D: 'static> {
    buffer: Vec<D>,
    shared: Rc<RefCell<Vec<Box<Push<(T, Content<D>)>>>>>,
}

impl<T: Clone+'static, D: Abomonation+Clone+'static> Push<(T, Content<D>)> for Tee<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<(T, Content<D>)>) {
        if let Some((ref time, ref mut data)) = *message {
            let mut pushers = self.shared.borrow_mut();
            for index in 0..pushers.len() {
                if index < pushers.len() - 1 {
                    // TODO : was `push_all`, but is now `extend`, slow.
                    self.buffer.extend_from_slice(data);
                    Content::push_at(&mut self.buffer, (*time).clone(), &mut pushers[index]);
                }
                else {
                    Content::push_at(data, (*time).clone(), &mut pushers[index]);
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
    /// Allocates a new pair of `Tee` and `TeeHelper`.
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

/// A shared list of `Box<Push>` used to add `Push` implementors.
pub struct TeeHelper<T, D> {
    shared: Rc<RefCell<Vec<Box<Push<(T, Content<D>)>>>>>
}

impl<T, D> TeeHelper<T, D> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<(T, Content<D>)>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

impl<T, D> Clone for TeeHelper<T, D> {
    fn clone(&self) -> Self {
        TeeHelper {
            shared: self.shared.clone()
        }
    }
}