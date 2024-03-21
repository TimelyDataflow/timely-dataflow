//! Merges the contents of multiple streams.


use crate::Container;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{StreamCore, Scope};

/// Merge the contents of two streams.
pub trait Concat<G: Scope, C: Container> {
    /// Merge the contents of two streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Concat, Inspect};
    ///
    /// timely::example(|scope| {
    ///
    ///     let stream = (0..10).to_stream(scope);
    ///     stream.concat(&stream)
    ///           .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concat(&self, _: &StreamCore<G, C>) -> StreamCore<G, C>;
}

impl<G: Scope, C: Container> Concat<G, C> for StreamCore<G, C> {
    fn concat(&self, other: &StreamCore<G, C>) -> StreamCore<G, C> {
        self.scope().concatenate([self.clone(), other.clone()])
    }
}

/// Merge the contents of multiple streams.
pub trait Concatenate<G: Scope, C: Container> {
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
    fn concatenate<I>(&self, sources: I) -> StreamCore<G, C>
    where
        I: IntoIterator<Item=StreamCore<G, C>>;
}

impl<G: Scope, C: Container> Concatenate<G, C> for StreamCore<G, C> {
    fn concatenate<I>(&self, sources: I) -> StreamCore<G, C>
    where
        I: IntoIterator<Item=StreamCore<G, C>>
    {
        let clone = self.clone();
        self.scope().concatenate(Some(clone).into_iter().chain(sources))
    }
}

impl<G: Scope, C: Container> Concatenate<G, C> for G {
    fn concatenate<I>(&self, sources: I) -> StreamCore<G, C>
    where
        I: IntoIterator<Item=StreamCore<G, C>>
    {

        // create an operator builder.
        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
        let mut builder = OperatorBuilder::new("Concatenate".to_string(), self.clone());

        // create new input handles for each input stream.
        let mut handles = sources.into_iter().map(|s| builder.new_input(&s, Pipeline)).collect::<Vec<_>>();

        // create one output handle for the concatenated results.
        let (mut output, result) = builder.new_output();

        // build an operator that plays out all input data.
        builder.build(move |_capability| {

            let mut vector = Default::default();
            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        data.swap(&mut vector);
                        output.session(&time).give_container(&mut vector);
                    })
                }
            }
        });

        result
    }
}
