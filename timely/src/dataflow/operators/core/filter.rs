//! Filters a stream by a predicate.
use crate::container::{Container, SizableContainer, PushInto};
use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter<C: Container> {
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::core::{Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter<P: FnMut(&C::Item<'_>)->bool+'static>(&self, predicate: P) -> Self;
}

impl<G: Scope, C: SizableContainer + Data> Filter<C> for StreamCore<G, C>
where
    for<'a> C: PushInto<C::Item<'a>>
{
    fn filter<P: FnMut(&C::Item<'_>)->bool+'static>(&self, mut predicate: P) -> StreamCore<G, C> {
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                if !data.is_empty() {
                    output.session(&time).give_iterator(data.drain().filter(&mut predicate));
                }
            });
        })
    }
}
