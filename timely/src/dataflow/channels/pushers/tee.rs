//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::{Bundle, Message};
use crate::Data;

use crate::communication::Push;

type PushList<T, D> = Rc<RefCell<Vec<Box<dyn Push<Bundle<T, D>>>>>>;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct Tee<T: 'static, D: 'static> {
    buffer: Vec<D>,
    shared: PushList<T, D>,
}

impl<T: Data, D: Data> Push<Bundle<T, D>> for Tee<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = message {
            for index in 1..pushers.len() {
                self.buffer.extend_from_slice(&message.data);
                Message::push_at(&mut self.buffer, message.time.clone(), &mut pushers[index-1]);
            }
        }
        else {
            for index in 1..pushers.len() {
                pushers[index-1].push(&mut None);
            }
        }
        if pushers.len() > 0 {
            let last = pushers.len() - 1;
            pushers[last].push(message);
        }
    }
}

impl<T, D> Tee<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (Tee<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = Tee {
            buffer: Vec::with_capacity(Message::<T, D>::default_length()),
            shared: shared.clone(),
        };

        (port, TeeHelper { shared })
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

impl<T, D> Debug for Tee<T, D>
where
    D: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Tee");
        debug.field("buffer", &self.buffer);

        if let Ok(shared) = self.shared.try_borrow() {
            debug.field("shared", &format!("{} pushers", shared.len()));
        } else {
            debug.field("shared", &"...");
        }

        debug.finish()
    }
}

/// A shared list of `Box<Push>` used to add `Push` implementors.
pub struct TeeHelper<T, D> {
    shared: PushList<T, D>,
}

impl<T, D> TeeHelper<T, D> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<Bundle<T, D>>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

impl<T, D> Clone for TeeHelper<T, D> {
    fn clone(&self) -> Self {
        TeeHelper {
            shared: self.shared.clone(),
        }
    }
}

impl<T, D> Debug for TeeHelper<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("TeeHelper");

        if let Ok(shared) = self.shared.try_borrow() {
            debug.field("shared", &format!("{} pushers", shared.len()));
        } else {
            debug.field("shared", &"...");
        }

        debug.finish()
    }
}
