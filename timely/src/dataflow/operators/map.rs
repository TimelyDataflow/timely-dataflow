//! Extension methods for `Stream` based on record-by-record transformation.

use timely_container::columnation::{Columnation, TimelyStack};
use timely_container::MonotonicContainer;
use crate::Data;
use crate::communication::message::RefOrMut;
use crate::dataflow::{Stream, Scope, StreamCore};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, D: Data> {
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
    fn map<D2: Data, L: FnMut(D)->D2+'static>(&self, logic: L) -> Stream<S, D2>;
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
    fn map_in_place<L: FnMut(&mut D)+'static>(&self, logic: L) -> Stream<S, D>;
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
    fn flat_map<I: IntoIterator, L: FnMut(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data;
}

impl<S: Scope, D: Data> Map<S, D> for Stream<S, D> {
    fn map<D2: Data, L: FnMut(D)->D2+'static>(&self, mut logic: L) -> Stream<S, D2> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Map", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain(..).map(|x| logic(x)));
            });
        })
    }
    fn map_in_place<L: FnMut(&mut D)+'static>(&self, mut logic: L) -> Stream<S, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "MapInPlace", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                for datum in vector.iter_mut() { logic(datum); }
                output.session(&time).give_vec(&mut vector);
            })
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: IntoIterator, L: FnMut(D)->I+'static>(&self, mut logic: L) -> Stream<S, I::Item> where I::Item: Data {
        let mut vector = Vec::new();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain(..).flat_map(|x| logic(x).into_iter()));
            });
        })
    }
}

/// Extension trait to `StreamCore` to map container elements while retaining the same container
/// type.
pub trait MapCore<S: Scope, C: MonotonicContainer<O>, O: Data> {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, MapCore, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_core(|x, _| *x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_core<L: FnMut(RefOrMut<C::Item>, Option<O>)->O+'static>(&self, logic: L) -> StreamCore<S, C::Output>;
}

impl<S: Scope, D: Data, O: Data> MapCore<S, Vec<D>, O> for StreamCore<S, Vec<D>> {
    fn map_core<L: FnMut(RefOrMut<D>, Option<O>)->O+'static>(&self, mut logic: L) -> StreamCore<S, Vec<O>> {
        let mut vector = Default::default();
        self.unary(Pipeline, "MapCore Vec", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain(..).map(|ref mut d| logic(RefOrMut::Mut(d), None)));
            });
        })
    }
}

impl<S: Scope, D: Data + Columnation, O: Data + Columnation> MapCore<S, TimelyStack<D>, O> for StreamCore<S, TimelyStack<D>> {
    fn map_core<L: FnMut(RefOrMut<D>, Option<O>)->O+'static>(&self, mut logic: L) -> StreamCore<S, TimelyStack<O>> {
        let mut output_buffer = TimelyStack::default();
        self.unary(Pipeline, "MapCore TimelyStack", move |_,_| move |input, output| {
            let mut last = None;
            while let Some((time, data)) = input.next() {
                for datum in &**data {
                    let o = logic(RefOrMut::Ref(datum), last);
                    output_buffer.copy(&o);
                    last = Some(o);
                }
                output.session(&time).give_container(&mut output_buffer);
            }
        })
    }
}
