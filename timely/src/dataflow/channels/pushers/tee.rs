//! The `Tee` and `TeeHelper` types, by which stream consumers rendezvous with the producer.
//!
//! The design is a shared list of `Box<dyn Push<T>>` types, which are added to by `TeeHelper`
//! and pushed into by the `Tee` once the dataflow is running. Some care is taken so that `T`
//! does not need to implement `Clone`, other than for the instantiation of a (boxed) variant
//! that supports multiple consumers, to avoid the constraint for single-consumer streams.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::Message;

use crate::communication::Push;
use crate::Container;

use push_set::{PushSet, PushOne, PushMany};
mod push_set {

    use crate::communication::Push;

    /// A type that can be pushed at, and which may be able to accommodate a similar pusher.
    ///
    /// This trait exists to support fanning out of pushers when the data may not be `Clone`,
    /// allowing the implementation for multiple pushers (which may require cloning) to be
    /// behind an abstraction.
    pub trait PushSet<T> : Push<T> {
        /// If a list of boxed pushers, that list.
        fn as_list(&mut self) -> Option<&mut Vec<Box<dyn Push<T>>>>;
    }

    /// A `Push` wrapper that implements `PushOne`.
    pub struct PushOne<P> { inner: P }
    impl<T, P: Push<T>> Push<T> for PushOne<P> {
        fn push(&mut self, item: &mut Option<T>) { self.inner.push(item) }
    }
    impl<T: 'static, P: Push<T> + 'static> PushSet<T> for PushOne<P> {
        fn as_list(&mut self) -> Option<&mut Vec<Box<dyn Push<T>>>> { None }
    }
    impl<P> From<P> for PushOne<P> { fn from(inner: P) -> Self { Self { inner } } }

    /// A `Push` wrapper for a list of boxed implementors.
    pub struct PushMany<T> {
        /// Used to clone into, for the chance to avoid continual re-allocation.
        buffer: Option<T>,
        /// The intended recipients of pushed values.
        list: Vec<Box<dyn Push<T>>>,
    }
    impl<T: Clone> Push<T> for PushMany<T> {
        fn push(&mut self, item: &mut Option<T>) {
            // We defensively clone `element` for all but the last element of `self.list`,
            // as we cannot be sure that a `Push` implementor will not modify the contents.
            // Indeed, that's the goal of the `Push` trait, to allow one to take ownership.

            // This guard prevents dropping `self.buffer` when a `None` is received.
            // We might prefer to do that, to reduce steady state memory.
            if item.is_some() {
                for pusher in self.list.iter_mut().rev().skip(1).rev() {
                    self.buffer.clone_from(&item);
                    pusher.push(&mut self.buffer);
                }
                if let Some(pusher) = self.list.last_mut() {
                    std::mem::swap(&mut self.buffer, item);
                    pusher.push(&mut self.buffer);
                }
            }
            else { for pusher in self.list.iter_mut() { pusher.done(); } }
        }
    }
    impl<T: Clone + 'static> PushSet<T> for PushMany<T> {
        fn as_list(&mut self) -> Option<&mut Vec<Box<dyn Push<T>>>> { Some(&mut self.list) }
    }
    impl<T> From<Vec<Box<dyn Push<T>>>> for PushMany<T> { fn from(list: Vec<Box<dyn Push<T>>>) -> Self { Self { list, buffer: None } } }

}

/// The shared state between a `Tee` and `TeeHelper`: an extensible list of pushers.
type PushList<T, C> = Rc<RefCell<Option<Box<dyn PushSet<Message<T, C>>>>>>;

/// The writing half of a shared destination for pushing at.
pub struct Tee<T, C> { shared: PushList<T, C> }

impl<T: 'static, C: Container> Push<Message<T, C>> for Tee<T, C> {
    #[inline]
    fn push(&mut self, message: &mut Option<Message<T, C>>) {
        if let Some(pushee) = self.shared.borrow_mut().as_mut() {
            pushee.push(message)
        }
    }
}

impl<T, C> Tee<T, C> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (Tee<T, C>, TeeHelper<T, C>) {
        let shared = Rc::new(RefCell::new(None));
        let port = Tee { shared: Rc::clone(&shared) };
        (port, TeeHelper { shared })
    }
}

impl<T, C> Debug for Tee<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "Tee") }
}

/// The subscribe half of a shared destination for pushing at.
///
/// Cloning a `TeeHelper` will upgrade it, teaching the shared list how to clone containers.
pub struct TeeHelper<T, C> { shared: PushList<T, C> }

impl<T: 'static, C: 'static> TeeHelper<T, C> {
    /// Upgrades the shared list to one that supports cloning.
    ///
    /// This method "teaches" the `Tee` how to clone containers, which enables adding multiple pushers.
    /// It introduces the cost of one additional virtual call through a boxed trait, so one should not
    /// upgrade for no reason.
    pub fn upgrade(&self) where T: Clone, C: Clone {
        let mut borrow = self.shared.borrow_mut();
        if let Some(mut pusher) = borrow.take() {
            if pusher.as_list().is_none() {
                *borrow = Some(Box::new(PushMany::from(vec![pusher as Box<dyn Push<Message<T, C>>>])));
            }
            else {
                *borrow = Some(pusher);
            }
        }
        else {
            *borrow = Some(Box::new(PushMany::from(vec![])));
        }
    }

    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<Message<T, C>>+'static>(self, pusher: P) {
        let mut borrow = self.shared.borrow_mut();
        if let Some(many) = borrow.as_mut() {
            many.as_list().unwrap().push(Box::new(pusher))
        }
        else {
            // If we are adding a second pusher without upgrading, something has gone wrong.
            assert!(borrow.is_none());
            *borrow = Some(Box::new(PushOne::from(pusher)));
        }
    }
}

impl<T: Clone+'static, C: Clone+'static> Clone for TeeHelper<T, C> {
    fn clone(&self) -> Self { self.upgrade(); TeeHelper { shared: Rc::clone(&self.shared) } }
}

impl<T, C> Debug for TeeHelper<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "TeeHelper") }
}
