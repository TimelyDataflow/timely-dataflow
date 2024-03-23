//! Extension traits for `Stream` implementing various operators.
//!
//! A collection of functions taking typed `Stream` objects as input and producing new `Stream`
//! objects as output. Many of the operators provide simple, composable functionality. Some of the
//! operators are more complicated, for use with advanced timely dataflow features.
//!
//! The [`Operator`](generic::operator) trait provides general
//! operators whose behavior can be supplied using closures accepting input and output handles.
//! Most of the operators in this module are defined using these two general operators.

pub use self::input::Input;
pub use self::unordered_input::UnorderedInput;
pub use self::partition::Partition;
pub use self::map::Map;
pub use self::inspect::{Inspect, InspectCore};
pub use self::filter::Filter;
pub use self::delay::Delay;
pub use self::exchange::Exchange;
pub use self::broadcast::Broadcast;
pub use self::branch::{Branch, BranchWhen};
pub use self::result::ResultStream;
pub use self::to_stream::ToStream;


pub use self::generic::Operator;
pub use self::generic::{Notificator, FrontierNotificator};

pub use self::reclock::Reclock;
pub use self::count::Accumulate;

pub mod core;

pub use self::core::enterleave::{self, Enter, Leave};
pub mod input;
pub mod flow_controlled;
pub mod unordered_input;
pub use self::core::feedback::{self, Feedback, LoopVariable, ConnectLoop};
pub use self::core::concat::{self, Concat, Concatenate};
pub mod partition;
pub mod map;
pub use self::core::inspect;
pub mod filter;
pub mod delay;
pub use self::core::exchange;
pub mod broadcast;
pub use self::core::probe::{self, Probe};
pub mod to_stream;
pub use self::core::capture::{self, Capture};
pub mod branch;
pub use self::core::ok_err::{self, OkErr};
pub use self::core::rc;
pub mod result;

pub mod aggregation;
pub mod generic;

pub use self::core::reclock;
pub mod count;

// keep "mint" module-private
mod capability;
pub use self::capability::{ActivateCapability, Capability, InputCapability, CapabilitySet, DowngradeError};
