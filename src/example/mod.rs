pub use self::stream::Stream;
pub use self::input::InputExtensionTrait;
pub use self::map::MapExt;
pub use self::filter::FilterExt;
pub use self::concat::{ConcatVecExt, ConcatExt};
pub use self::feedback::FeedbackExt;
pub use self::graph_builder::{EnterSubgraphExt, LeaveSubgraphExt};
pub use self::unary::UnaryExt;
pub use self::inspect::{InspectExt, InspectBatchExt};

pub mod input;
pub mod concat;
pub mod feedback;
pub mod queue;
pub mod stream;
pub mod barrier;
pub mod graph_builder;
// pub mod command;     // awaiting old_io -> io completion.
pub mod map;
pub mod flat_map;
pub mod filter;
pub mod inspect;

pub mod distinct;
pub mod unary;
pub mod partition;
