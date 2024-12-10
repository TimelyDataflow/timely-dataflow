//! Merges the contents of multiple streams.


use crate::Container;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{OwnedStream, StreamLike, Scope};

/// Merge the contents of two streams.
pub trait Concat<G: Scope, C: Container, S: StreamLike<G, C>> {
    /// Merge the contents of two streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Concat, Inspect};
    ///
    /// timely::example(|scope| {
    ///
    ///     let stream = (0..10).to_stream(scope).tee();
    ///     stream.concat(&stream)
    ///           .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concat(self, other: S) -> OwnedStream<G, C>;
}

impl<G: Scope, C: Container + 'static, S: StreamLike<G, C>> Concat<G, C, S> for S {
    fn concat(self, other: S) -> OwnedStream<G, C> {
        self.scope().concatenate([self, other])
    }
}

/// Merge the contents of multiple streams.
pub trait Concatenate<G: Scope, C: Container, S: StreamLike<G, C>> {
    /// Merge the contents of multiple streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Concatenate, Inspect};
    ///
    /// timely::example(|scope| {
    ///
    ///     let streams = vec![(0..10).to_stream(scope),
    ///                        (0..10).to_stream(scope),
    ///                        (0..10).to_stream(scope)];
    ///
    ///     scope.concatenate(streams)
    ///          .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concatenate<I>(self, sources: I) -> OwnedStream<G, C>
    where
        I: IntoIterator<Item=S>;
}

impl<G: Scope, C: Container + 'static> Concatenate<G, C, OwnedStream<G, C>> for OwnedStream<G, C> {
    fn concatenate<I>(self, sources: I) -> OwnedStream<G, C>
    where
        I: IntoIterator<Item=OwnedStream<G, C>>
    {
        self.scope().concatenate(Some(self).into_iter().chain(sources))
    }
}

impl<G: Scope, C: Container + 'static, S: StreamLike<G, C>> Concatenate<G, C, S> for &G {
    fn concatenate<I>(self, sources: I) -> OwnedStream<G, C>
    where
        I: IntoIterator<Item=S>
    {

        // create an operator builder.
        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
        let mut builder = OperatorBuilder::new("Concatenate".to_string(), self.clone());

        // create new input handles for each input stream.
        let mut handles = sources.into_iter().map(|s| builder.new_input(s, Pipeline)).collect::<Vec<_>>();

        // create one output handle for the concatenated results.
        let (mut output, result) = builder.new_output();

        // build an operator that plays out all input data.
        builder.build(move |_capability| {

            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        output.session(&time).give_container(data);
                    })
                }
            }
        });

        result
    }
}
