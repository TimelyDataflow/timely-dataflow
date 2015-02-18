use progress::{Timestamp, PathSummary, Scope, Subgraph};
use progress::subgraph::{Source, Target};
use progress::broadcast::Progcaster;

// cloneable so that we can make some copies, let different streams call connect.
pub trait Graph : Clone+'static {
    type Timestamp : Timestamp;
    type Summary : PathSummary<Self::Timestamp>;

    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp, Self::Summary>>) -> u64;
    fn new_subgraph<T: Timestamp, S: PathSummary<T>>(&mut self, default: T, progcaster: Progcaster<(Self::Timestamp,T)>) ->
        Subgraph<Self::Timestamp, Self::Summary, T, S>;

    // fn graph_clone(&self) -> Self;
}

pub trait GraphExtension<G: Graph> {
    fn add_scope<SC: Scope<G::Timestamp, G::Summary>>(&mut self, scope: SC) -> u64;
}

impl<G: Graph> GraphExtension<G> for G {
    fn add_scope<SC: Scope<G::Timestamp, G::Summary>>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
}
