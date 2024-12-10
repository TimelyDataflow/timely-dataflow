//! Extension methods for `Stream` based on record-by-record transformation.

use crate::dataflow::{OwnedStream, StreamLike, Scope};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::operators::core::{Map as MapCore};

/// Extension trait for `Stream`.
pub trait Map<G: Scope, D: 'static> : Sized {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<D2: 'static, L: FnMut(D)->D2+'static>(self, mut logic: L) -> OwnedStream<G, Vec<D2>> {
        self.flat_map(move |x| std::iter::once(logic(x)))
    }
    /// Updates each element of the stream and yields the element, re-using memory where possible.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_in_place(|x| *x += 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_in_place<L: FnMut(&mut D)+'static>(self, logic: L) -> OwnedStream<G, Vec<D>>;
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<I: IntoIterator, L: FnMut(D)->I+'static>(self, logic: L) -> OwnedStream<G, Vec<I::Item>> where I::Item: 'static;
}

impl<G: Scope, D: 'static, S: StreamLike<G, Vec<D>>> Map<G, D> for S {
    fn map_in_place<L: FnMut(&mut D)+'static>(self, mut logic: L) -> OwnedStream<G, Vec<D>> {
        self.unary(Pipeline, "MapInPlace", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                for datum in data.iter_mut() { logic(datum); }
                output.session(&time).give_container(data);
            })
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: IntoIterator, L: FnMut(D)->I+'static>(self, logic: L) -> OwnedStream<G, Vec<I::Item>> where I::Item: 'static {
        MapCore::flat_map(self, logic)
    }
}
