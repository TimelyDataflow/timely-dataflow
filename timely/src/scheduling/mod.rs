//! Types and traits to activate and schedule fibers.

pub mod activate;

pub use self::activate::{Activations, Activator, ActivateOnDrop, SyncActivator};

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
