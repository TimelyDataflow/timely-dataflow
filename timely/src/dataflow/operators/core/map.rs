//! Extension methods for `StreamCore` based on record-by-record transformation.

use crate::container::{Container, SizableContainer, PushInto};
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, C: Container> {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::core::{Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map(|x| x + 1)
    ///            .container::<Vec<_>>()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<C2, D2, L>(&self, mut logic: L) -> StreamCore<S, C2>
    where
        C2: SizableContainer + PushInto<D2>,
        L: FnMut(C::Item<'_>)->D2 + 'static,
    {
        self.flat_map(move |x| std::iter::once(logic(x)))
    }
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::core::{Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .container::<Vec<_>>()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<C2, I, L>(&self, logic: L) -> StreamCore<S, C2>
    where
        I: IntoIterator,
        C2: SizableContainer + PushInto<I::Item>,
        L: FnMut(C::Item<'_>)->I + 'static,
    ;
}

impl<S: Scope, C: Container> Map<S, C> for StreamCore<S, C> {
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<C2, I, L>(&self, mut logic: L) -> StreamCore<S, C2>
    where
        I: IntoIterator,
        C2: SizableContainer + PushInto<I::Item>,
        L: FnMut(C::Item<'_>)->I + 'static,
    {
        let mut container = Default::default();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut container);
                output.session(&time).give_iterator(container.drain().flat_map(&mut logic));
            });
        })
    }
}
