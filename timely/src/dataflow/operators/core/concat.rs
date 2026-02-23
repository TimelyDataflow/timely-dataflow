//! Merges the contents of multiple streams.

use crate::Container;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope};

/// Merge the contents of two streams.
pub trait Concat<G: Scope, C> {
    /// Merge the contents of two streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Concat, Inspect};
    ///
    /// timely::example(|scope| {
    ///
    ///     let stream = (0..10).to_stream(scope);
    ///     stream.clone()
    ///           .concat(stream)
    ///           .container::<Vec<_>>()
    ///           .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concat(self, other: Stream<G, C>) -> Stream<G, C>;
}

impl<G: Scope, C: Container> Concat<G, C> for Stream<G, C> {
    fn concat(self, other: Stream<G, C>) -> Stream<G, C> {
        self.scope().concatenate([self, other])
    }
}

/// Merge the contents of multiple streams.
pub trait Concatenate<G: Scope, C> {
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
    ///          .container::<Vec<_>>()
    ///          .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concatenate<I>(&self, sources: I) -> Stream<G, C>
    where
        I: IntoIterator<Item=Stream<G, C>>;
}

impl<G: Scope, C: Container> Concatenate<G, C> for G {
    fn concatenate<I>(&self, sources: I) -> Stream<G, C>
    where
        I: IntoIterator<Item=Stream<G, C>>
    {

        // create an operator builder.
        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
        let mut builder = OperatorBuilder::new("Concatenate".to_string(), self.clone());
        builder.set_notify(false);

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
                        output.give(&time, data);
                    })
                }
            }
        });

        result
    }
}
