//! Types and traits to activate and schedule fibers.

use std::rc::Rc;
use std::cell::RefCell;

pub mod activate;

pub use self::activate::{Activations, ActivationHandle};

/// A type that can be scheduled.
pub trait Schedule {
    /// A descriptive name for the operator
    fn name(&self) -> &str;
    /// An address identifying the operator.
    fn path(&self) -> &[usize];
    /// Schedules the operator, receives "cannot terminate" boolean.
    ///
    /// The return value indicates whether `self` has outstanding
    /// work and would be upset if the computation terminated.
    fn schedule(&mut self) -> bool;
}

/// Methods for types which schedule fibers.
pub trait Scheduler {
    /// Provides a shared handle to the activation scheduler.
    fn activations(&self) -> Rc<RefCell<Activations>>;
}