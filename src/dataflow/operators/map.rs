//! Extension methods for `Stream` based on record-by-record transformation.

use Data;
use dataflow::{Stream, Scope};
use dataflow::channels::pact::Pipeline;
use dataflow::operators::unary::Unary;
use drain::DrainExt;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, D: Data> {
    /// Consumes each element of the stream and yields a new element.
    fn map<D2: Data, L: Fn(D)->D2+'static>(&self, logic: L) -> Stream<S, D2>;
    /// Updates each element of the stream and yields the element.
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<S, D>;
    /// Consumes each element of the stream and yields some number of new elements.
    fn flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data;
    ///
    fn filter_map<D2: Data, L: Fn(D)->Option<D2>+'static>(&self, logic: L) -> Stream<S, D2>;
}

impl<S: Scope, D: Data> Map<S, D> for Stream<S, D> {
    fn map<D2: Data, L: Fn(D)->D2+'static>(&self, logic: L) -> Stream<S, D2> {
        self.unary_stream(Pipeline, "Map", move |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(time).give_iterator(data.drain_temp().map(|x| logic(x)));
            }
        })
    }
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<S, D> {
        self.unary_stream(Pipeline, "MapInPlace", move |input, output| {
            while let Some((time, data)) = input.next() {
                for datum in data.iter_mut() { logic(datum); }
                output.session(time).give_content(data);
            }
        })
    }
    fn flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data {
        self.unary_stream(Pipeline, "FlatMap", move |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(time).give_iterator(data.drain_temp().flat_map(|x| logic(x)));
            }
        })
    }
    fn filter_map<D2: Data, L: Fn(D)->Option<D2>+'static>(&self, logic: L) -> Stream<S, D2> {
        self.unary_stream(Pipeline, "FilterMap", move |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(time).give_iterator(data.drain_temp().filter_map(|x| logic(x)));
            }
        })
    }
}
