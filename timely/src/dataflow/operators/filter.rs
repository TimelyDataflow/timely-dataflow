//! Filters a stream by a predicate.

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{OwnedStream, StreamLike, Scope};
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter<G: Scope, D: 'static> {
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter<P: FnMut(&D)->bool+'static>(self, predicate: P) -> OwnedStream<G, Vec<D>>;
}

impl<G: Scope, D: 'static, S: StreamLike<G, Vec<D>>> Filter<G, D> for S {
    fn filter<P: FnMut(&D)->bool+'static>(self, mut predicate: P) -> OwnedStream<G, Vec<D>> {
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.retain(|x| predicate(x));
                if !data.is_empty() {
                    output.session(&time).give_container(data);
                }
            });
        })
    }
}
