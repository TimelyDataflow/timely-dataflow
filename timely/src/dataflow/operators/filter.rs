//! Filters a stream by a predicate.

use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::operator::Operator;

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
    fn filter<P: FnMut(&D)->bool+'static>(self, predicate: P) -> Self;
}

impl<G: Scope, D: Data> Filter<D> for Stream<G, D> {
    fn filter<P: FnMut(&D)->bool+'static>(self, mut predicate: P) -> Stream<G, D> {
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each_time(|time, data| {
                let mut session = output.session(&time);
                for data in data {
                    data.retain(&mut predicate);
                    session.give_container(data);
                }
            });
        })
    }
}
