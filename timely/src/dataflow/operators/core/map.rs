//! Extension methods for [`StreamCore`] based on record-by-record transformation.

use timely_container::{Container, PushContainer, PushInto};
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, C: Container> {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::core::Map;
    /// use timely::dataflow::operators::{ToStream, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map::<Vec<_>, _, _>(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<C2: PushContainer, D2: PushInto<C2>, L: 'static>(&self, logic: L) -> StreamCore<S, C2>
        where
                for<'a> L: FnMut(C::Item<'a>)->D2;
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::core::Map;
    /// use timely::dataflow::operators::{ToStream, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map::<Vec<_>, _, _>(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<C2: PushContainer, I: IntoIterator, L: 'static>(&self, logic: L) -> StreamCore<S, C2>
        where
            I::Item: PushInto<C2>,
            for<'a> L: FnMut(C::Item<'a>)->I;
}

impl<S: Scope, C: Container> Map<S, C> for StreamCore<S, C> {
    fn map<C2: PushContainer, D2: PushInto<C2>, L: 'static>(&self, mut logic: L) -> StreamCore<S, C2>
        where
                for<'a> L: FnMut(C::Item<'a>)->D2,
    {
        let mut vector = Default::default();
        self.unary(Pipeline, "Map", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain().map(&mut logic));
            });
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<C2: PushContainer, I: IntoIterator, L: 'static>(&self, mut logic: L) -> StreamCore<S, C2>
        where
            I::Item: PushInto<C2>,
            for<'a> L: FnMut(C::Item<'a>)->I,
    {
        let mut vector = Default::default();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain().flat_map(|x| logic(x).into_iter()));
            });
        })
    }
}
