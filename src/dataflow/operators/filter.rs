//! Filters a stream by a predicate.

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

/// Extension trait for filtering.
pub trait Filter<D: Data> {
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter<L: Fn(&D)->bool+'static>(&self, predicate: L) -> Self;
}

impl<G: Scope, D: Data> Filter<D> for Stream<G, D> {
    fn filter<L: Fn(&D)->bool+'static>(&self, predicate: L) -> Stream<G, D> {
        self.unary_stream(Pipeline, "Filter", move |input, output| {
            while let Some((time, data)) = input.next() {
                data.retain(|x| predicate(x));
                if data.len() > 0 {
                    output.session(&time).give_content(data);
                }
            }
        })
    }
}
