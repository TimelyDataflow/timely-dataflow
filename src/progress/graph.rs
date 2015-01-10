use progress::{Timestamp, PathSummary, Scope};
use progress::subgraph::{Source, Target};

// cloneable so that we can make some copies, let different streams call connect.
pub trait Graph<T: Timestamp, S: PathSummary<T>> : 'static
{
    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<T, S>>) -> u64;

    // Box<Graph<T,S>>.clone() doesn't seem to work.
    // perhaps this should be with the Stream code?
    fn as_box(&self) -> Box<Graph<T, S>>;
}

pub trait GraphExtension<T: Timestamp, S: PathSummary<T>>
{
    fn add_scope<SC: Scope<T, S>>(&mut self, scope: SC) -> u64;
}

impl<T: Timestamp, S: PathSummary<T>, G: Graph<T, S>+?Sized> GraphExtension<T, S> for G
{
    fn add_scope<SC: Scope<T, S>>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
}
