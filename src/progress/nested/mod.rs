//! Coordination of progress information between a scope-as-operator and its children operators.

pub use self::subgraph::{Subgraph, SubgraphBuilder};

pub mod pointstamp_counter;
pub mod subgraph;
pub mod reachability;
