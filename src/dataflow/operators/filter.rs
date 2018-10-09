//! Filters a stream by a predicate.

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter<D: Data> {
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
    fn filter(&self, predicate: impl Fn(&D)->bool+'static) -> Self;
}

impl<G: Scope, D: Data> Filter<D> for Stream<G, D> {
    fn filter(&self, predicate: impl Fn(&D)->bool+'static) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                vector.retain(|x| predicate(x));
                if vector.len() > 0 {
                    output.session(&time).give_vec(&mut vector);
                }
            });
        })
    }
}
