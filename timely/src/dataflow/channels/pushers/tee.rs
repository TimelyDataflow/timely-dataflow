//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::Message;
use crate::communication::Push;
use crate::Data;
use crate::container::WithProgress;

type PushList<T, C> = Rc<RefCell<Vec<Box<dyn Push<Message<T, C>>>>>>;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct Tee<T, C> {
    buffer: C,
    shared: PushList<T, C>,
}

impl<T: Data, C: WithProgress + Data> Push<Message<T, C>> for Tee<T, C> {
    #[inline]
    fn push(&mut self, message: &mut Option<Message<T, C>>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = message {
            for index in 1..pushers.len() {
                self.buffer.clone_from(&message.data);
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

impl<T, C: WithProgress> Tee<T, C> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (Tee<T, C>, TeeHelper<T, C>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = Tee {
            buffer: Default::default(),
            shared: Rc::clone(&shared),
        };

        (port, TeeHelper { shared })
    }
}

impl<T, C: WithProgress> Clone for Tee<T, C> {
    fn clone(&self) -> Self {
        Self {
            buffer: Default::default(),
            shared: Rc::clone(&self.shared),
        }
    }
}

impl<T, C> Debug for Tee<T, C>
where
    C: Debug,
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
pub struct TeeHelper<T, C> {
    shared: PushList<T, C>,
}

impl<T, C> TeeHelper<T, C> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<Message<T, C>>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

impl<T, C> Clone for TeeHelper<T, C> {
    fn clone(&self) -> Self {
        TeeHelper {
            shared: Rc::clone(&self.shared),
        }
    }
}

impl<T, C> Debug for TeeHelper<T, C> {
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
