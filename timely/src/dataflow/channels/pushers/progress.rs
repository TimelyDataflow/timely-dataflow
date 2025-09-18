//! A wrapper that allows containers to be sent by validating capabilities.

use std::rc::Rc;
use std::cell::RefCell;

use crate::progress::{ChangeBatch, Timestamp};
use crate::dataflow::channels::Message;
use crate::dataflow::operators::CapabilityTrait;
use crate::communication::Push;
use crate::Container;

/// A wrapper that allows containers to be sent by validating capabilities.
#[derive(Debug)]
pub struct Progress<T, P> {
    pushee: P,
    internal: Rc<RefCell<ChangeBatch<T>>>,
    port: usize,
}

impl<T: Timestamp, P> Progress<T, P> {
    /// Ships a container using a provided capability.
    ///
    /// On return, the container may hold undefined contents and should be cleared before it is reused.
    #[inline] pub fn give<C: Container, CT: CapabilityTrait<T>>(&mut self, capability: &CT, container: &mut C) where P: Push<Message<T, C>> {
        debug_assert!(self.valid(capability), "Attempted to open output session with invalid capability");
        if !container.is_empty() { Message::push_at(container, capability.time().clone(), &mut self.pushee); }
    }
    /// Activates a `Progress` into a `ProgressSession` which will flush when dropped.
    pub fn activate<'a, C>(&'a mut self) -> ProgressSession<'a, T, C, P> where P: Push<Message<T, C>> {
        ProgressSession {
            borrow: self,
            marker: std::marker::PhantomData,
        }
    }
    /// Determines if the capability is valid for this output.
    pub fn valid<CT: CapabilityTrait<T>>(&self, capability: &CT) -> bool {
        capability.valid_for_output(&self.internal, self.port)
    }
}

impl<T, P> Progress<T, P> where T : Ord+Clone+'static {
    /// Allocates a new `Progress` from a pushee and capability validation information.
    pub fn new(pushee: P, internal: Rc<RefCell<ChangeBatch<T>>>, port: usize) -> Self {
        Self { pushee, internal, port }
    }
}

/// A session that provides access to a `Progress` but will flush when dropped.
///
/// The type of the container `C` must be known, as long as the flushing action requires a specific `Push` implementation.
pub struct ProgressSession<'a, T: Timestamp, C, P: Push<Message<T, C>>> {
    borrow: &'a mut Progress<T, P>,
    marker: std::marker::PhantomData<C>,
}

impl<'a, T: Timestamp, C, P: Push<Message<T, C>>> std::ops::Deref for ProgressSession<'a, T, C, P> {
    type Target = Progress<T, P>;
    fn deref(&self) -> &Self::Target { self.borrow }
}

impl<'a, T: Timestamp, C, P: Push<Message<T, C>>> std::ops::DerefMut for ProgressSession<'a, T, C, P> {
    fn deref_mut(&mut self) -> &mut Self::Target { self.borrow }
}

impl<'a, T: Timestamp, C, P: Push<Message<T, C>>> Drop for ProgressSession<'a, T, C, P> {
    fn drop(&mut self) { self.borrow.pushee.push(&mut None); }
}
