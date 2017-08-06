//! Merges the contents of multiple streams.


use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::generic::binary::Binary;

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
        self.binary_stream(other, Pipeline, Pipeline, "Concat", |input1, input2, output| {
            input1.for_each(|time, data| { output.session(&time).give_content(data); });
            input2.for_each(|time, data| { output.session(&time).give_content(data); });
        })
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
    fn concatenate(&self, Vec<Stream<G, D>>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Concatenate<G, D> for G {
    fn concatenate(&self, mut sources: Vec<Stream<G, D>>) -> Stream<G, D> {
        if let Some(mut result) = sources.pop() {
            while let Some(next) = sources.pop() {
                result = result.concat(&next);
            }
            result
        }
        else {
            panic!("must pass at least one stream to concatenate");
        }
    }
}
