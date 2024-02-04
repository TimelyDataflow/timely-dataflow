//! Filters a stream by a predicate.

use timely_container::{Container, PushContainer, PushInto};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter<C: Container> {
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
    fn filter<P: 'static>(&self, predicate: P) -> Self
    where
        for<'a> P: FnMut(&C::Item<'a>)->bool;
}

impl<G: Scope, C: PushContainer> Filter<C> for StreamCore<G, C>
where
    for<'a> C::Item<'a>: PushInto<C>,
{
    fn filter<P>(&self, mut predicate: P) -> StreamCore<G, C>
    where
        for<'a> P: FnMut(&C::Item<'a>)->bool+'static
    {
        let mut vector = Default::default();
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                if !vector.is_empty() {
                    output.session(&time).give_iterator(vector.drain().filter(|x| predicate(&x)));
                }
            });
        })
    }
}
