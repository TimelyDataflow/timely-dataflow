//! Extension traits for `Stream` and `StreamVec` implementing various operators.
//!
//! A collection of functions taking typed stream objects as input and producing new stream
//! objects as output. Many of the operators provide simple, composable functionality. Some of the
//! operators are more complicated, for use with advanced timely dataflow features.
//!
//! The [`core`](core) module defines operators that work for streams of general containers.
//! The [`vec`](vec) module defines operators that work for streams of vector containers.
//!
//! The [`Operator`](generic::operator) trait provides general
//! operators whose behavior can be supplied using closures accepting input and output handles.
//! Most of the operators in this module are defined using these two general operators.

pub use self::inspect::{Inspect, InspectCore};
pub use self::exchange::Exchange;

pub use self::generic::Operator;
pub use self::generic::{Notificator, FrontierNotificator};

pub use self::reclock::Reclock;

pub mod core;
pub mod vec;

pub use self::core::enterleave::{self, Enter, Leave};
pub use self::core::feedback::{self, Feedback, LoopVariable, ConnectLoop};
pub use self::core::concat::{self, Concat, Concatenate};
pub use self::core::inspect;
pub use self::core::exchange;
pub use self::core::probe::{self, Probe};
pub use self::core::capture::{self, Capture};
pub use self::core::ok_err::{self, OkErr};
pub use self::core::rc;
pub use self::core::to_stream::ToStream;
pub use self::core::input::Input;

pub mod generic;

pub use self::core::reclock;

// keep "mint" module-private
mod capability;
pub use self::capability::{ActivateCapability, Capability, CapabilityTrait, InputCapability, CapabilitySet, DowngradeError};
