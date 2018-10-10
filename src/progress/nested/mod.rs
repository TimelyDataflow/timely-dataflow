//! Coordination of progress information between a scope-as-operator and its children operators.

pub use self::subgraph::{Subgraph, SubgraphBuilder};
pub use self::subgraph::{Source, Target};

pub mod pointstamp_counter;
pub mod product;
pub mod subgraph;
pub mod reachability;
pub mod reachability_neu;
