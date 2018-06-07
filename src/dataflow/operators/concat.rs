//! Merges the contents of multiple streams.


use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};

/// Merge the contents of two streams.
pub trait Concat<G: Scope, D: Data> {
    /// Merge the contents of two streams.
    ///
    /// #Examples
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
    fn concat(&self, &Stream<G, D>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Concat<G, D> for Stream<G, D> {
    fn concat(&self, other: &Stream<G, D>) -> Stream<G, D> {
        self.scope().concatenate(vec![self.clone(), other.clone()])
    }
}

/// Merge the contents of multiple streams.
pub trait Concatenate<G: Scope, D: Data> {
    /// Merge the contents of multiple streams.
    ///
    /// #Examples
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
    fn concatenate(&self, impl IntoIterator<Item=Stream<G, D>>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Concatenate<G, D> for G {
    fn concatenate(&self, sources: impl IntoIterator<Item=Stream<G, D>>) -> Stream<G, D> {

        // create an operator builder.
        use dataflow::operators::generic::builder_rc::OperatorBuilder;
        let mut builder = OperatorBuilder::new("Concatenate".to_string(), self.clone());

        // create new input handles for each input stream.
        let mut handles = sources.into_iter().map(|s| builder.new_input(&s, Pipeline)).collect::<Vec<_>>();

        // create one output handle for the concatenated results.
        let (mut output, result) = builder.new_output();

        // build an operator that plays out all input data.
        builder.build(move |_capability| {
            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        output.session(&time).give_content(data);
                    })
                }
            }
        });

        result
    }
}
