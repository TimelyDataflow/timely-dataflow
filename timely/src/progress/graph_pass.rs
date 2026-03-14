//! Graph transformation pass API for subgraph optimization.
//!
//! A `GraphPass` receives the graph topology (children and edges) and can transform
//! it before the reachability tracker is built. This allows optimizations like
//! operator fusion to be implemented as pluggable passes, decoupled from the
//! progress tracking code in `subgraph.rs`.

use crate::progress::{Source, Target, Timestamp};
use super::subgraph::PerOperatorState;

/// A graph transformation pass that runs during `SubgraphBuilder::build()`.
///
/// Passes receive the children vector and edge list, and may transform them
/// in place. A pass that merges operators should tombstone the absorbed
/// operators (setting their inputs/outputs to 0, clearing their operator,
/// and optionally setting `forward_to` for activation forwarding).
///
/// Passes run sequentially in registration order, each seeing the output
/// of the previous pass.
pub(crate) trait GraphPass<T: Timestamp> {
    /// Transform the graph topology in place.
    ///
    /// The `children` vector is indexed by operator index (child 0 is the
    /// subgraph boundary). The `edges` vector contains all (source, target)
    /// pairs representing dataflow connections.
    ///
    /// Implementations may:
    /// * Modify operator state (replace operators, change port counts)
    /// * Add or remove edges
    /// * Tombstone operators by clearing their fields and setting `forward_to`
    ///
    /// Implementations must preserve the length of `children` (indices are
    /// used by the reachability tracker).
    fn apply(&self, children: &mut Vec<PerOperatorState<T>>, edges: &mut Vec<(Source, Target)>);
}
