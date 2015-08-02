//! Operators for `Stream` manipulation.
//!
//! A collection of functions taking typed `Stream` objects as input and producing new `Stream`
//! objects as output. Many of the operators provide simple, composable functionality. Some of the
//! operators are more complicated, for use with advanced timely dataflow features.
//!
//! The [`Unary`](./unary/index.html) and [`Binary`](./binary/index.html) operators provide general
//! operators whose behavior can be supplied using closures accepting input and output handles.
//! Most of the operators in this module are defined using these two general operators.

pub use self::enterleave::*;
pub use self::unary::Extension as UnaryExtension;
// pub use self::distinct::*;
pub use self::queue::*;
pub use self::input::*;
pub use self::feedback::Extension as FeedbackExtension;
pub use self::feedback::ConnectExtension as FeedbackConnectExtension;
pub use self::concat::*;
pub use self::partition::Extension as PartitionExtension;
pub use self::map::*;
pub use self::map_in_place::*;
pub use self::inspect::Inspect;
pub use self::flat_map::*;
pub use self::filter::*;
pub use self::binary::Extension as BinaryExtension;
pub use self::delay::Delay;
pub use self::exchange::Exchange;
pub use self::probe::Extension as ProbeExtension;

pub mod enterleave;
pub mod unary;
// pub mod distinct;
pub mod queue;
pub mod input;
pub mod feedback;
pub mod concat;
pub mod partition;
pub mod map;
pub mod map_in_place;
pub mod inspect;
pub mod flat_map;
pub mod filter;
pub mod binary;
pub mod delay;
pub mod exchange;
pub mod probe;
