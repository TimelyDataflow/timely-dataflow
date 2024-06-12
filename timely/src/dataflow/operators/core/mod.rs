//! Extension traits for `StreamCore` implementing various operators that
//! are independent of specific container types.

pub mod capture;
pub mod concat;
pub mod enterleave;
pub mod exchange;
pub mod feedback;
pub mod filter;
pub mod input;
pub mod inspect;
pub mod map;
pub mod ok_err;
pub mod probe;
pub mod rc;
pub mod reclock;
pub mod to_stream;
pub mod unordered_input;

pub use capture::Capture;
pub use concat::{Concat, Concatenate};
pub use enterleave::{Enter, Leave};
pub use exchange::Exchange;
pub use feedback::{Feedback, LoopVariable, ConnectLoop};
pub use filter::Filter;
pub use input::Input;
pub use inspect::{Inspect, InspectCore};
pub use map::Map;
pub use ok_err::OkErr;
pub use probe::Probe;
pub use to_stream::{ToStream, ToStreamBuilder};
pub use reclock::Reclock;
pub use unordered_input::{UnorderedInput, UnorderedHandle};
