//! Capabilities to send data from operators
//!
//! Timely dataflow operators are only able to send data if they possess a "capability",
//! a system-created object which warns the runtime that the operator may still produce
//! output records. 
//! 
//! The timely dataflow runtime creates a capability and provides it to an operator whenever
//! the operator receives input data. The capabilities allow the operator to respond to the
//! received data, immediately or in the future, for as long as the capability is held.
//!
//! Timely dataflow's progress tracking infrastructure communicates the number of outstanding
//! capabilities across all workers. 
//! Each operator may hold on to its capabilities, and may clone, advance, and drop them. 
//! Each of these actions informs the timely dataflow runtime of changes to the number of outstanding
//! capabilities, so that the runtime can notice when the count for some capability reaches zero. 
//! While an operator can hold capabilities indefinitely, and create as many new copies of them
//! as it would like, the progress tracking infrastructure will not move forward until the 
//! operators eventually release their capabilities.
//!
//! Note that these capabilities currently lack the property of "transferability": 
//! An operator should not hand its capabilities to some other operator. In the future, we should
//! probably bind capabilities more strongly to a specific operator and output.

use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use progress::Timestamp;
use progress::count_map::CountMap;
use std::fmt::{self, Debug};

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

    /// Makes a new capability for a timestamp `new_time` greater or equal to the timestamp of
    /// the source capability (`self`).
    #[inline]
    pub fn delayed(&self, new_time: &T) -> Capability<T> {
        assert!(new_time >= &self.time);
        mint(*new_time, self.internal.clone())
    }
}

/// Creates a new capability at `t` while incrementing (and keeping a reference to) the provided
/// `CountMap`.
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

impl<T: Timestamp> Debug for Capability<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Capability {{ time: {:?}, internal: ... }}", self.time)
    }
}
