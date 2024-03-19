//! Extension traits for `Stream` implementing various operators that
//! are independent of specific container types.

pub mod concat;
pub mod delay;
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

pub use concat::{Concat, Concatenate};
pub use delay::Delay;
pub use exchange::Exchange;
pub use self::feedback::{Feedback, LoopVariable, ConnectLoop};
pub use filter::Filter;
pub use input::{Input, Handle as InputHandle};
pub use inspect::{Inspect, InspectCore};
pub use map::Map;
pub use ok_err::OkErr;
pub use probe::{Probe, Handle};
pub use rc::SharedStream;
pub use reclock::Reclock;
pub use to_stream::ToStream;
pub use unordered_input::{UnorderedHandle, UnorderedInput};
