//! A `Push` implementor with a single target.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use timely_communication::Push;
use crate::dataflow::channels::Message;

/// A pusher that can bind to a single downstream pusher.
pub struct PushOwned<T, D>(Rc<RefCell<Option<Box<dyn Push<Message<T, D>>>>>>);

impl<T, D> PushOwned<T, D> {
    /// Create a new `PushOwned`. Similarly to `Tee`, it returns a pair where either element
    /// can be used as pusher or registrar.
    pub fn new() -> (Self, Self) {
        let shared = Rc::new(RefCell::new(None));
        (Self(Rc::clone(&shared)), Self(shared))
    }

    /// Set the downstream pusher.
    ///
    /// Consumes `Self` as only a single pusher can be set.
    pub fn set_pusher<P: Push<Message<T, D>> + 'static>(self, pusher: P) {
        *self.0.borrow_mut() = Some(Box::new(pusher));
    }
}

impl<T, D> fmt::Debug for PushOwned<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushOwned").finish_non_exhaustive()
    }
}

impl<T, D> Push<Message<T, D>> for PushOwned<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<Message<T, D>>) {
        let mut pusher = self.0.borrow_mut();
        if let Some(pusher) = pusher.as_mut() {
            pusher.push(message);
        }
    }
}
