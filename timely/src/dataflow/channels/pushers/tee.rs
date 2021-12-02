//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::{BundleCore, Message};

use crate::{Container, Data};
use crate::communication::Push;
use crate::communication::message::RefOrMut;

type PushList<T, D> = Rc<RefCell<Vec<(Box<dyn Push<BundleCore<T, D>>>, Box<dyn FnMut(&T, RefOrMut<D>, &mut D)>)>>>;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct TeeCore<T, D> {
    buffer: D,
    shared: PushList<T, D>,
}

/// [TeeCore] specialized to `Vec`-based container.
pub type Tee<T, D> = TeeCore<T, Vec<D>>;

impl<T: Data, D: Container> Push<BundleCore<T, D>> for TeeCore<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<BundleCore<T, D>>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = message {
            for index in 1..pushers.len() {
                (pushers[index-1].1)(&message.time, RefOrMut::Ref(&message.data), &mut self.buffer);
                if !self.buffer.is_empty() {
                    Message::push_at(&mut self.buffer, message.time.clone(), &mut pushers[index-1].0);
                }
            }
        }
        else {
            for index in 1..pushers.len() {
                pushers[index-1].0.push(&mut None);
            }
        }
        if pushers.len() > 0 {
            let last = pushers.len() - 1;
            if let Some(message) = message {
                let message = message.as_ref_or_mut();
                let time = message.time.clone();
                let data = match message {
                    RefOrMut::Ref(message) => RefOrMut::Ref(&message.data),
                    RefOrMut::Mut(message) => RefOrMut::Mut(&mut message.data)
                };
                (pushers[last].1)(&time, data, &mut self.buffer);
                if !self.buffer.is_empty() {
                    Message::push_at(&mut self.buffer, time, &mut pushers[last].0);
                }
            } else {
                pushers[last].0.push(message);
            }
        }
    }
}

impl<T, D: Container> TeeCore<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (TeeCore<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = TeeCore {
            buffer: Default::default(),
            shared: shared.clone(),
        };

        (port, TeeHelper { shared })
    }
}

impl<T, D: Container> Clone for TeeCore<T, D> {
    fn clone(&self) -> Self {
        Self {
            buffer: Default::default(),
            shared: self.shared.clone(),
        }
    }
}

impl<T, D> Debug for TeeCore<T, D>
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
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`, with an
    /// associated `filter` which will be applied before pushing data..
    pub fn add_pusher<P, F>(&self, pusher: P, filter: F)
        where
            P: Push<BundleCore<T, D>> + 'static,
            F: FnMut(&T, RefOrMut<D>, &mut D) + 'static
    {
        self.shared.borrow_mut().push((Box::new(pusher), Box::new(filter)));
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
