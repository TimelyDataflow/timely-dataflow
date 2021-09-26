//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::{BundleCore, Message};
use crate::{Data};

use crate::communication::{Push, Container};
use crate::communication::message::RefOrMut;
use crate::communication::message::IntoAllocated;

type PushList<T, D> = Rc<RefCell<Vec<Box<dyn Push<BundleCore<T, D>>>>>>;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct TeeCore<T: 'static, D: Container+'static> {
    shared: PushList<T, D>,
}

/// [TeeCore] specialized to `Vec`-based container.
pub type Tee<T, D> = TeeCore<T, Vec<D>>;

impl<T: Data, D: Container> Push<BundleCore<T, D>> for TeeCore<T, D> {
    #[inline]
    fn push(&mut self, message: Option<BundleCore<T, D>>, allocation: &mut Option<<BundleCore<T, D> as Container>::Allocation>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = &message {
            let mut inner_allocation: Option<D::Allocation> = allocation.take().and_then(|m| m.0).map(|m| m.0);
            for index in 1..pushers.len() {
                let copy: D = if let Some(allocation) = inner_allocation.take() {
                    allocation.assemble(RefOrMut::Ref(&message.data))
                } else {
                    D::Allocation::assemble_new(RefOrMut::Ref(&message.data))
                };
                Message::push_at(Some(copy), message.time.clone(), &mut pushers[index-1], &mut inner_allocation);
            }
        }
        else {
            for index in 1..pushers.len() {
                pushers[index-1].push(None, &mut None);
            }
        }
        if pushers.len() > 0 {
            let last = pushers.len() - 1;
            pushers[last].push(message, &mut None);
        }
    }
}

impl<T, D: Container> TeeCore<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (TeeCore<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = TeeCore {
            shared: shared.clone(),
        };

        (port, TeeHelper { shared })
    }
}

impl<T, D: Container> Debug for TeeCore<T, D>
where
    D: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Tee");

        if let Ok(shared) = self.shared.try_borrow() {
            debug.field("shared", &format!("{} pushers", shared.len()));
        } else {
            debug.field("shared", &"...");
        }

        debug.finish()
    }
}

/// A shared list of `Box<Push>` used to add `Push` implementors.
pub struct TeeHelper<T, D: Container> {
    shared: PushList<T, D>,
}

impl<T, D: Container> TeeHelper<T, D> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<BundleCore<T, D>>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

impl<T, D: Container> Clone for TeeHelper<T, D> {
    fn clone(&self) -> Self {
        TeeHelper {
            shared: self.shared.clone(),
        }
    }
}

impl<T, D: Container> Debug for TeeHelper<T, D> {
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
