//! Extension methods for `Stream` based on record-by-record transformation.

use crate::{Container, Data};
use crate::communication::Push;
use crate::container::{AppendableContainer, RefOrMut};
use crate::container::columnation::{Columnation, TimelyStack};
use crate::dataflow::{Stream, Scope, StreamCore};
use crate::dataflow::channels::BundleCore;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::channels::pushers::buffer::Session;
use crate::dataflow::channels::pushers::{CounterCore, TeeCore};
use crate::dataflow::operators::generic::operator::Operator;
use crate::progress::Timestamp;

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

/// Extension trait for `Stream`.
pub trait MapRefCore<S: Scope, D, C2: Container, P: Push<BundleCore<S::Timestamp, C2>>> {
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, MapRefCore, Inspect, Sink};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_into(|x, session| (0..*x).into_iter().sink(session))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_into<L: FnMut(&D, &mut Session<S::Timestamp, C2, P>)+'static>(&self, logic: L) -> StreamCore<S, C2>;
}

impl<S: Scope, D: Columnation + Data, D2: Columnation + Data> MapRefCore<S, D, TimelyStack<D2>, CounterCore<S::Timestamp, TimelyStack<D2>, TeeCore<S::Timestamp, TimelyStack<D2>>>> for StreamCore<S, TimelyStack<D>> {
    fn map_into<L: FnMut(&D, &mut Session<S::Timestamp, TimelyStack<D2>, CounterCore<S::Timestamp, TimelyStack<D2>, TeeCore<S::Timestamp, TimelyStack<D2>>>>) + 'static>(&self, mut logic: L) -> StreamCore<S, TimelyStack<D2>> {
        let mut vector = Default::default();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                let mut session = output.session(&time);
                for datum in &*vector {
                    logic(datum, &mut session);
                }
            }
        })
    }
}

impl<S: Scope, D: Data, D2: Data> MapRefCore<S, D, Vec<D2>, CounterCore<S::Timestamp, Vec<D2>, TeeCore<S::Timestamp, Vec<D2>>>> for Stream<S, D> {
    fn map_into<L: FnMut(&D, &mut Session<S::Timestamp, Vec<D2>, CounterCore<S::Timestamp, Vec<D2>, TeeCore<S::Timestamp, Vec<D2>>>>) + 'static>(&self, mut logic: L) -> Stream<S, D2> {
        let mut vector = Default::default();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                let mut session = output.session(&time);
                for datum in &vector {
                    logic(datum, &mut session);
                }
            }
        })
    }
}

/// Sink data into a session.
///
/// The primary use case for this trait is to extend iterators with a `sink` method to write their
/// contents into a session. This variant accepts owned values.
pub trait Sink<T: Timestamp, C: Container, D> {
    /// Sink all elements from the iterator into the `session`, presenting owned values.
    fn sink(self, session: &mut Session<T, C, CounterCore<T, C, TeeCore<T, C>>>);
}

impl<T: Timestamp, C: AppendableContainer, I: IntoIterator<Item=C::Item>> Sink<T, C, C::Item> for I
where
    C::Item: Data+Default,
{
    fn sink(self, session: &mut Session<T, C, CounterCore<T, C, TeeCore<T, C>>>) {
        for item in self {
            session.show(RefOrMut::Owned(item))
        }
    }
}

/// Sink data into a session.
///
/// The primary use case for this trait is to extend iterators with a `sink` method to write their
/// contents into a session. This variant accepts references to values.
pub trait SinkRef<T: Timestamp, C: Container, D> {
    /// Sink all elements from the iterator into the `session`, presenting references.
    fn sink_ref(self, session: &mut Session<T, C, CounterCore<T, C, TeeCore<T, C>>>);
}

impl<'a, T: Timestamp, C: AppendableContainer, I: IntoIterator<Item=&'a C::Item>> SinkRef<T, C, C::Item> for I
where
    C::Item: Data+Default,
{
    fn sink_ref(self, session: &mut Session<T, C, CounterCore<T, C, TeeCore<T, C>>>) {
        for item in self {
            session.show(RefOrMut::Ref(item))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::container::columnation::TimelyStack;
    use crate::container::RefOrMut;
    use crate::dataflow::operators::InspectCore;
    use crate::dataflow::operators::map::{Sink, SinkRef};

    #[test]
    fn test_map_core_timely_stack() {
        use crate::dataflow::operators::{ToStreamCore, MapRefCore};

        crate::example(|scope| {
            Some((0..10u32).into_iter().collect::<Vec<_>>()).to_stream_core(scope)
                .map_into(|x, session| (0..*x).sink(session))
                .inspect_container(|x| {
                    x.ok().map(|x| println!("seen: {:?}", x.1));
                });
            Some((0..10u32).into_iter().collect::<Vec<_>>()).to_stream_core(scope)
                .map_into(|x, session| Some(*x).iter().sink_ref(session))
                .inspect_container(|x| {
                    x.ok().map(|x| println!("seen: {:?}", x.1));
                });
            Some((0..10u32).into_iter().collect::<Vec<_>>()).to_stream_core(scope)
                .map_into(|x, session| Some(*x).into_iter().for_each(|x| session.show(RefOrMut::Owned(x))))
                .inspect_container(|x| {
                    x.ok().map(|x| println!("seen: {:?}", x.1));
                });
            Some((0..10u32).into_iter().collect::<TimelyStack<_>>()).to_stream_core(scope)
                .map_into(|x, session| (0..*x).sink(session))
                .inspect_container(|x| {
                    x.ok().map(|x| println!("seen: {:?}", x.1));
                });
            Some((0..10u32).into_iter().collect::<TimelyStack<_>>()).to_stream_core(scope)
                .map_into(|x, session| Some(*x).iter().sink_ref(session))
                .inspect_container(|x| {
                    x.ok().map(|x| println!("seen: {:?}", x.1));
                });
            Some((0..10u32).into_iter().collect::<TimelyStack<_>>()).to_stream_core(scope)
                .map_into(|x, session| Some(*x).iter().for_each(|x| session.show(RefOrMut::Ref(x))))
                .inspect_container(|x| {
                    x.ok().map(|x| println!("seen: {:?}", x.1));
                });
        });
    }
}
