//! Extension methods and implementations for streams of vector containers.
//!
//! These methods operate on containers that are vectors of individual items,
//! and much of their behavior can be described item-by-item.
//!
//! Vector containers are a natural entry point before one forms a stronger
//! opinion on the containers one would like to use. While they are easy to
//! use, vector containers (and owned data) invite performance antipatterns
//! around resource management (allocation and deallocation, across threads).

pub mod input;
pub mod flow_controlled;
pub mod unordered_input;
pub mod partition;
pub mod map;
pub mod filter;
pub mod delay;
pub mod broadcast;
pub mod to_stream;
pub mod branch;
pub mod result;
pub mod aggregation;
pub mod count;

pub use self::input::Input;
pub use self::unordered_input::UnorderedInput;
pub use self::partition::Partition;
pub use self::map::Map;
pub use self::filter::Filter;
pub use self::delay::Delay;
pub use self::broadcast::Broadcast;
pub use self::branch::{Branch, BranchWhen};
pub use self::result::ResultStream;
pub use self::to_stream::ToStream;
