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
use std::fmt::{self, Debug};

use crate::order::PartialOrder;
use crate::progress::Timestamp;
use crate::progress::ChangeBatch;
use crate::scheduling::Activations;

/// An internal trait expressing the capability to send messages with a given timestamp.
pub trait CapabilityTrait<T: Timestamp> {
    /// The timestamp associated with the capability.
    fn time(&self) -> &T;
    fn valid_for_output(&self, query_buffer: &Rc<RefCell<ChangeBatch<T>>>) -> bool;
}

impl<'a, T: Timestamp, C: CapabilityTrait<T>> CapabilityTrait<T> for &'a C {
    fn time(&self) -> &T { (**self).time() }
    fn valid_for_output(&self, query_buffer: &Rc<RefCell<ChangeBatch<T>>>) -> bool {
        (**self).valid_for_output(query_buffer)
    }
}
impl<'a, T: Timestamp, C: CapabilityTrait<T>> CapabilityTrait<T> for &'a mut C {
    fn time(&self) -> &T { (**self).time() }
    fn valid_for_output(&self, query_buffer: &Rc<RefCell<ChangeBatch<T>>>) -> bool {
        (**self).valid_for_output(query_buffer)
    }
}

/// The capability to send data with a certain timestamp on a dataflow edge.
///
/// Capabilities are used by timely dataflow's progress tracking machinery to restrict and track
/// when user code retains the ability to send messages on dataflow edges. All capabilities are
/// constructed by the system, and should eventually be dropped by the user. Failure to drop
/// a capability (for whatever reason) will cause timely dataflow's progress tracking to stall.
pub struct Capability<T: Timestamp> {
    time: T,
    internal: Rc<RefCell<ChangeBatch<T>>>,
}

impl<T: Timestamp> CapabilityTrait<T> for Capability<T> {
    fn time(&self) -> &T { &self.time }
    fn valid_for_output(&self, query_buffer: &Rc<RefCell<ChangeBatch<T>>>) -> bool {
        Rc::ptr_eq(&self.internal, query_buffer)
    }
}

impl<T: Timestamp> Capability<T> {
    /// The timestamp associated with this capability.
    #[inline]
    pub fn time(&self) -> &T {
        &self.time
    }

    /// Makes a new capability for a timestamp `new_time` greater or equal to the timestamp of
    /// the source capability (`self`).
    ///
    /// This method panics if `self.time` is not less or equal to `new_time`.
    #[inline]
    pub fn delayed(&self, new_time: &T) -> Capability<T> {
        if !self.time.less_equal(new_time) {
            panic!("Attempted to delay {:?} to {:?}, which is not `less_equal` the capability's time.", self, new_time);
        }
        mint(new_time.clone(), self.internal.clone())
    }

    /// Downgrades the capability to one corresponding to `new_time`.
    ///
    /// This method panics if `self.time` is not less or equal to `new_time`.
    #[inline]
    pub fn downgrade(&mut self, new_time: &T) {
        let new_cap = self.delayed(new_time);
        *self = new_cap;
    }
}

/// Creates a new capability at `t` while incrementing (and keeping a reference to) the provided
/// `ChangeBatch`.
/// Declared separately so that it can be kept private when `Capability` is re-exported.
#[inline]
pub fn mint<T: Timestamp>(time: T, internal: Rc<RefCell<ChangeBatch<T>>>) -> Capability<T> {
    internal.borrow_mut().update(time.clone(), 1);
    Capability {
        time,
        internal,
    }
}

// Necessary for correctness. When a capability is dropped, the "internal" `ChangeBatch` needs to be
// updated accordingly to inform the rest of the system that the operator has released its permit
// to send data and request notification at the associated timestamp.
impl<T: Timestamp> Drop for Capability<T> {
    #[inline]
    fn drop(&mut self) {
        self.internal.borrow_mut().update(self.time.clone(), -1);
    }
}

impl<T: Timestamp> Clone for Capability<T> {
    #[inline]
    fn clone(&self) -> Capability<T> {
        mint(self.time.clone(), self.internal.clone())
    }
}

impl<T: Timestamp> Deref for Capability<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.time
    }
}

impl<T: Timestamp> Debug for Capability<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Capability {{ time: {:?}, internal: ... }}", self.time)
    }
}

impl<T: Timestamp> PartialEq for Capability<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.time() == other.time() && Rc::ptr_eq(&self.internal, &other.internal)
    }
}
impl<T: Timestamp> Eq for Capability<T> { }

impl<T: Timestamp> PartialOrder for Capability<T> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.time().less_equal(other.time()) && Rc::ptr_eq(&self.internal, &other.internal)
    }
}

impl<T: Timestamp> ::std::hash::Hash for Capability<T> {
    #[inline]
    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
        self.time.hash(state);
    }
}

/// An unowned capability, which can be used but not retained.
///
/// The capability reference supplies a `retain(self)` method which consumes the reference
/// and turns it into an owned capability
pub struct CapabilityRef<'cap, T: Timestamp+'cap> {
    time: &'cap T,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>,
}

impl<'cap, T: Timestamp+'cap> CapabilityTrait<T> for CapabilityRef<'cap, T> {
    fn time(&self) -> &T { self.time }
    fn valid_for_output(&self, query_buffer: &Rc<RefCell<ChangeBatch<T>>>) -> bool {
        // let borrow = ;
        self.internal.borrow().iter().any(|rc| Rc::ptr_eq(rc, query_buffer))
    }
}

impl<'cap, T: Timestamp+'cap> CapabilityRef<'cap, T> {
    /// The timestamp associated with this capability.
    #[inline]
    pub fn time(&self) -> &T {
        self.time
    }

    /// Makes a new capability for a timestamp `new_time` greater or equal to the timestamp of
    /// the source capability (`self`).
    ///
    /// This method panics if `self.time` is not less or equal to `new_time`.
    #[inline]
    pub fn delayed(&self, new_time: &T) -> Capability<T> {
        self.delayed_for_output(new_time, 0)
    }

    /// Delays capability for a specific output port.
    pub fn delayed_for_output(&self, new_time: &T, output_port: usize) -> Capability<T> {
        // TODO : Test operator summary?
        if !self.time.less_equal(new_time) {
            panic!("Attempted to delay {:?} to {:?}, which is not `less_equal` the capability's time.", self, new_time);
        }
        if output_port < self.internal.borrow().len() {
            mint(new_time.clone(), self.internal.borrow()[output_port].clone())
        }
        else {
            panic!("Attempted to acquire a capability for a non-existent output port.");
        }
    }

    /// Transform to an owned capability.
    ///
    /// This method produces an owned capability which must be dropped to release the
    /// capability. Users should take care that these capabilities are only stored for
    /// as long as they are required, as failing to drop them may result in livelock.
    #[inline]
    pub fn retain(self) -> Capability<T> {
        // mint(self.time.clone(), self.internal)
        self.retain_for_output(0)
    }

    /// Transforms to an owned capability for a specific output port.
    pub fn retain_for_output(self, output_port: usize) -> Capability<T> {
        if output_port < self.internal.borrow().len() {
            mint(self.time.clone(), self.internal.borrow()[output_port].clone())
        }
        else {
            panic!("Attempted to acquire a capability for a non-existent output port.");
        }
    }
}

impl<'cap, T: Timestamp> Deref for CapabilityRef<'cap, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        self.time
    }
}

impl<'cap, T: Timestamp> Debug for CapabilityRef<'cap, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CapabilityRef {{ time: {:?}, internal: ... }}", self.time)
    }
}


/// Creates a new capability at `t` while incrementing (and keeping a reference to) the provided
/// `ChangeBatch`.
/// Declared separately so that it can be kept private when `Capability` is re-exported.
#[inline]
pub fn mint_ref<'cap, T: Timestamp>(time: &'cap T, internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>) -> CapabilityRef<'cap, T> {
    CapabilityRef {
        time,
        internal,
    }
}

/// Capability that activates on drop.
#[derive(Clone)]
pub struct ActivateCapability<T: Timestamp> {
    pub(crate) capability: Capability<T>,
    pub(crate) address: Rc<Vec<usize>>,
    pub(crate) activations: Rc<RefCell<Activations>>,
}

impl<T: Timestamp> CapabilityTrait<T> for ActivateCapability<T> {
    fn time(&self) -> &T { self.capability.time() }
    fn valid_for_output(&self, query_buffer: &Rc<RefCell<ChangeBatch<T>>>) -> bool {
        self.capability.valid_for_output(query_buffer)
    }
}

impl<T: Timestamp> ActivateCapability<T> {
    /// Creates a new activating capability.
    pub fn new(capability: Capability<T>, address: &[usize], activations: Rc<RefCell<Activations>>) -> Self {
        Self {
            capability,
            address: Rc::new(address.to_vec()),
            activations,
        }
    }
    /// The timestamp associated with this capability.
    pub fn time(&self) -> &T {
        self.capability.time()
    }
    /// Creates a new delayed capability.
    pub fn delayed(&self, time: &T) -> Self {
        ActivateCapability {
            capability: self.capability.delayed(time),
            address: self.address.clone(),
            activations: self.activations.clone(),
        }
    }
    /// Downgrades this capability.
    pub fn downgrade(&mut self, time: &T) {
        self.capability.downgrade(time);
        self.activations.borrow_mut().activate(&self.address[..]);
    }
}

impl<T: Timestamp> Drop for ActivateCapability<T> {
    fn drop(&mut self) {
        self.activations.borrow_mut().activate(&self.address[..]);
    }
}

/// A set of capabilities, for possibly incomparable times.
#[derive(Clone, Debug)]
pub struct CapabilitySet<T: Timestamp> {
    elements: Vec<Capability<T>>,
}

impl<T: Timestamp> CapabilitySet<T> {

    /// Allocates an empty capability set.
    pub fn new() -> Self {
        CapabilitySet { elements: Vec::new() }
    }

    /// Allocates a capability set containing a single capability.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::{
    ///     operators::{ToStream, generic::Operator},
    ///     channels::pact::Pipeline,
    /// };
    /// use timely::dataflow::operators::CapabilitySet;
    ///
    /// timely::example(|scope| {
    ///     vec![()].into_iter().to_stream(scope)
    ///         .unary_frontier(Pipeline, "example", |default_cap, _info| {
    ///             let mut cap = CapabilitySet::from_elem(default_cap);
    ///             let mut vector = Vec::new();
    ///             move |input, output| {
    ///                 cap.downgrade(&input.frontier().frontier());
    ///                 while let Some((time, data)) = input.next() {
    ///                     data.swap(&mut vector);
    ///                 }
    ///                 let a_cap = cap.first();
    ///                 if let Some(a_cap) = a_cap.as_ref() {
    ///                     output.session(a_cap).give(());
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    pub fn from_elem(cap: Capability<T>) -> Self {
        CapabilitySet { elements: vec![cap] }
    }

    /// Inserts `capability` into the set, discarding redundant capabilities.
    pub fn insert(&mut self, capability: Capability<T>) {
        if !self.elements.iter().any(|c| c.less_equal(&capability)) {
            self.elements.retain(|c| !capability.less_equal(c));
            self.elements.push(capability);
        }
    }

    /// Creates a new capability to send data at `time`.
    ///
    /// This method panics if there does not exist a capability in `self.elements` less or equal to `time`.
    pub fn delayed(&self, time: &T) -> Capability<T> {
        self.elements.iter().find(|c| c.time().less_equal(time)).unwrap().delayed(time)
    }

    /// Downgrades the set of capabilities to correspond with the times in `frontier`.
    ///
    /// This method panics if any element of `frontier` is not greater or equal to some element of `self.elements`.
    pub fn downgrade<B, F>(&mut self, frontier: F)
    where
        B: std::borrow::Borrow<T>,
        F: IntoIterator<Item=B>,
    {
        let count = self.elements.len();
        for time in frontier.into_iter() {
            let capability = self.delayed(time.borrow());
            self.elements.push(capability);
        }
        self.elements.drain(..count);
    }
}

impl<T: Timestamp> Deref for CapabilitySet<T> {
    type Target=[Capability<T>];

    fn deref(&self) -> &[Capability<T>] {
        &self.elements[..]
    }
}
