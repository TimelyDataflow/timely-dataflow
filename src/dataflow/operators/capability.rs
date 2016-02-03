use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use progress::Timestamp;
use progress::count_map::CountMap;

/// A capability for timestamp `t` represents a permit for an operator that holds the capability
/// to send data and request notifications at timestamp `t`.
pub struct Capability<T: Timestamp> {
    time: T,
    internal: Rc<RefCell<CountMap<T>>>,
}

impl<T: Timestamp> Capability<T> {
    /// The timestamp associated with this capability.
    #[inline]
    pub fn time(&self) -> T {
        self.time
    }

    /// Makes a new capability for a timestamp that's greater then the timestamp associated with
    /// the source capability (`self`).
    #[inline]
    pub fn delayed(&self, new_time: &T) -> Capability<T> {
        assert!(new_time >= &self.time);
        mint(*new_time, self.internal.clone())
    }
}

/// Creates a new capability at `t` while incrementing (and keeping a reference to) the provided
/// CountMap.
/// Declared separately so that it can be kept private when `Capability` is re-exported.
pub fn mint<T: Timestamp>(time: T, internal: Rc<RefCell<CountMap<T>>>) -> Capability<T> {
    internal.borrow_mut().update(&time, 1);
    Capability {
        time: time,
        internal: internal
    }
}

// Necessary for correctness. When a capability is dropped, the "internal" `CountMap` needs to be
// updated accordingly to inform the rest of the system that the operator has released its permit
// to send data and request notification at the associated timestamp.
impl<T: Timestamp> Drop for Capability<T> {
    fn drop(&mut self) {
        self.internal.borrow_mut().update(&self.time, -1);
    }
}

impl<T: Timestamp> Clone for Capability<T> {
    fn clone(&self) -> Capability<T> {
        mint(self.time, self.internal.clone())
    }
}

impl<T: Timestamp> Deref for Capability<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.time
    }
}
