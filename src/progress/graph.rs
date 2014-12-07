use progress::{Timestamp, PathSummary, Scope};
use progress::subgraph::{Source, Target};

// cloneable so that we can make some copies, let different streams call connect.
pub trait Graph<T: Timestamp, S: PathSummary<T>> : Clone+'static
{
    fn connect(&mut self, source: Source, target: Target);
    fn add_scope(&mut self, scope: Box<Scope<T, S>>) -> uint;

    // Box<Graph<T,S>>.clone() doesn't seem to work.
    // perhaps this should be with the Stream code?
    fn as_box(&self) -> Box<Graph<T, S>>;
}
