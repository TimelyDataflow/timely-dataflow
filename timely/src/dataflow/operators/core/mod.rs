//! Extension traits for `StreamCore` implementing various operators that
//! are independent of specific container types.

pub mod concat;
pub mod enterleave;
pub mod exchange;
pub mod feedback;
pub mod filter;
pub mod inspect;
pub mod map;
pub mod ok_err;
pub mod probe;
pub mod rc;
pub mod reclock;

pub use concat::{Concat, Concatenate};
pub use enterleave::{Enter, Leave};
pub use exchange::Exchange;
pub use feedback::{Feedback, LoopVariable, ConnectLoop};
pub use filter::Filter;
pub use inspect::{Inspect, InspectCore};
pub use map::Map;
pub use ok_err::OkErr;
pub use probe::Probe;
pub use reclock::Reclock;
