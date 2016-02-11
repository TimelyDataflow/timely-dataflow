//! Extension methods for `Stream` based on record-by-record transformation.

use Data;
use dataflow::{Stream, Scope};
use dataflow::channels::pact::Pipeline;
use dataflow::operators::unary::Unary;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, D: Data> {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<D2: Data, L: Fn(D)->D2+'static>(&self, logic: L) -> Stream<S, D2>;
    /// Updates each element of the stream and yields the element, re-using memory where possible.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_in_place(|x| *x += 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<S, D>;
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data;
}

impl<S: Scope, D: Data> Map<S, D> for Stream<S, D> {
    fn map<D2: Data, L: Fn(D)->D2+'static>(&self, logic: L) -> Stream<S, D2> {
        self.unary_stream(Pipeline, "Map", move |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(&time).give_iterator(data.drain(..).map(|x| logic(x)));
            }
        })
    }
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<S, D> {
        self.unary_stream(Pipeline, "MapInPlace", move |input, output| {
            while let Some((time, data)) = input.next() {
                for datum in data.iter_mut() { logic(datum); }
                output.session(&time).give_content(data);
            }
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data {
        self.unary_stream(Pipeline, "FlatMap", move |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(&time).give_iterator(data.drain(..).flat_map(|x| logic(x)));
            }
        })
    }
    // fn filter_map<D2: Data, L: Fn(D)->Option<D2>+'static>(&self, logic: L) -> Stream<S, D2> {
    //     self.unary_stream(Pipeline, "FilterMap", move |input, output| {
    //         while let Some((time, data)) = input.next() {
    //             output.session(time).give_iterator(data.drain(..).filter_map(|x| logic(x)));
    //         }
    //     })
    // }
}
