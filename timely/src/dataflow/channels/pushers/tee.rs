//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::{BundleCore, Message, MessageAllocation};
use crate::{Container, ContainerBuilder, Data};

use crate::communication::Push;

type PushList<T, D, A> = Rc<RefCell<Vec<Box<dyn Push<BundleCore<T, D>, A>>>>>;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct TeeCore<T: 'static, D: 'static, A: 'static> {
    shared: PushList<T, D, A>,
}

/// [TeeCore] specialized to `Vec`-based container.
pub type Tee<T, D> = TeeCore<T, Vec<D>, Vec<D>>;

impl<T: Data, D: Container> Push<BundleCore<T, D>, MessageAllocation<D::Allocation>> for TeeCore<T, D, MessageAllocation<D::Allocation>> {
    #[inline]
    fn push(&mut self, message: Option<BundleCore<T, D>>, allocation: &mut Option<MessageAllocation<D::Allocation>>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = &message {
            let mut allocation = None;
            for index in 1..pushers.len() {
                let copy = if let Some(allocation) = allocation.take() {
                    message.data.clone_into(allocation)
                } else {
                    message.data.clone()
                };
                Message::push_at(Some(copy), message.time.clone(), &mut pushers[index-1], &mut allocation);
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

impl<T, D: Container> TeeCore<T, D, MessageAllocation<D::Allocation>> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (TeeCore<T, D, MessageAllocation<D::Allocation>>, TeeHelper<T, D, MessageAllocation<D::Allocation>>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = TeeCore {
            shared: shared.clone(),
        };

        (port, TeeHelper { shared })
    }
}

impl<T, D, A> Debug for TeeCore<T, D, A>
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
pub struct TeeHelper<T, D, A> {
    shared: PushList<T, D, A>,
}

impl<T, D, A> TeeHelper<T, D, A> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<BundleCore<T, D>, A>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

impl<T, D, A> Clone for TeeHelper<T, D, A> {
    fn clone(&self) -> Self {
        TeeHelper {
            shared: self.shared.clone(),
        }
    }
}

impl<T, D, A> Debug for TeeHelper<T, D, A> {
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
