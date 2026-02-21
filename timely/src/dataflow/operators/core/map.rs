//! Extension methods for `StreamCore` based on record-by-record transformation.

use crate::container::{DrainContainer, SizableContainer, PushInto};
use crate::Container;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, C: DrainContainer> : Sized {
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
    fn map<C2, D2, L>(self, mut logic: L) -> StreamCore<S, C2>
    where
        C2: Container + SizableContainer + PushInto<D2>,
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
    fn flat_map<C2, I, L>(self, logic: L) -> StreamCore<S, C2>
    where
        I: IntoIterator,
        C2: Container + SizableContainer + PushInto<I::Item>,
        L: FnMut(C::Item<'_>)->I + 'static,
    ;

    /// Creates a `FlatMapBuilder`, which allows chaining of iterator logic before finalization into a stream.
    ///
    /// This pattern exists to make it easier to provide the ergonomics of iterator combinators without the
    /// overhead of multiple dataflow operators. The resulting single operator will internally use compiled
    /// iterators to go record-by-record, and unlike a chain of operators will not need to stage the records
    /// of intermediate stages.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Capture, ToStream, core::Map};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let data = timely::example(|scope| {
    ///     (0..10i32)
    ///         .to_stream(scope)
    ///         .flat_map_builder(|x| x + 1)
    ///         .map(|x| x + 1)
    ///         .map(|x| x + 1)
    ///         .map(|x| x + 1)
    ///         .map(Some)
    ///         .into_stream::<_,Vec<i32>>()
    ///         .capture()
    /// });
    ///
    /// assert_eq!((4..14).collect::<Vec<_>>(), data.extract()[0].1);
    /// ```
    fn flat_map_builder<I, L>(self, logic: L) -> FlatMapBuilder<Self, C, L, I>
    where
        L: for<'a> Fn(C::Item<'a>) -> I,
    {
        FlatMapBuilder::new(self, logic)
    }
}

impl<S: Scope, C: Container + DrainContainer> Map<S, C> for StreamCore<S, C> {
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<C2, I, L>(self, mut logic: L) -> StreamCore<S, C2>
    where
        I: IntoIterator,
        C2: Container + SizableContainer + PushInto<I::Item>,
        L: FnMut(C::Item<'_>)->I + 'static,
    {
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each_time(|time, data| {
                output.session(&time)
                      .give_iterator(data.flat_map(|d| d.drain()).flat_map(&mut logic));
            });
        })
    }
}


/// A stream wrapper that allows the accumulation of flatmap logic.
pub struct FlatMapBuilder<T, C: DrainContainer, F: 'static, I>
where
    for<'a> F: Fn(C::Item<'a>) -> I,
{
    stream: T,
    logic: F,
    marker: std::marker::PhantomData<C>,
}

impl<T, C: DrainContainer, F, I> FlatMapBuilder<T, C, F, I>
where
    for<'a> F: Fn(C::Item<'a>) -> I,
{
    /// Create a new wrapper with no action on the stream.
    pub fn new(stream: T, logic: F) -> Self {
        FlatMapBuilder { stream, logic, marker: std::marker::PhantomData }
    }

    /// Transform a flatmapped stream through additional logic.
    pub fn map<G: Fn(I) -> I2 + 'static, I2>(self, g: G) -> FlatMapBuilder<T, C, impl Fn(C::Item<'_>) -> I2 + 'static, I2> {
        let logic = self.logic;
        FlatMapBuilder {
            stream: self.stream,
            logic: move |x| g(logic(x)),
            marker: std::marker::PhantomData,
        }
    }
    /// Convert the wrapper into a stream.
    pub fn into_stream<S, C2>(self) -> StreamCore<S, C2>
    where
        I: IntoIterator,
        S: Scope,
        T: Map<S, C>,
        C2: Container + SizableContainer + PushInto<I::Item>,
    {
        Map::flat_map(self.stream, self.logic)
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::operators::{Capture, ToStream, core::Map};
    use crate::dataflow::operators::capture::Extract;

    #[test]
    fn test_builder() {
        let data = crate::example(|scope| {
            let stream = (0..10i32).to_stream(scope);
            stream.flat_map_builder(|x| x + 1)
                .map(|x| x + 1)
                .map(|x| x + 1)
                .map(|x| x + 1)
                .map(Some)
                .into_stream::<_,Vec<i32>>()
                .capture()
        });

        assert_eq!((4..14).collect::<Vec<_>>(), data.extract()[0].1);
    }
}