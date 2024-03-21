//! Extension traits for `Stream` implementing various operators that
//! are independent of specific container types.

pub mod concat;
pub mod exchange;
pub mod feedback;
pub mod inspect;
pub mod rc;
pub mod reclock;

pub use concat::{Concat, Concatenate};
pub use exchange::Exchange;
pub use feedback::{Feedback, LoopVariable, ConnectLoop};
pub use inspect::{Inspect, InspectCore};
pub use reclock::Reclock;
