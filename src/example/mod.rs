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
// pub mod iterate;     // having a hard time making usable.
