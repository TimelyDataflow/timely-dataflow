//! Merges the contents of multiple streams.


use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{CoreStream, Scope};
use crate::communication::{Container, IntoAllocated};

/// Merge the contents of two streams.
pub trait Concat<G: Scope, D: Container> {
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
    fn concat(&self, _: &CoreStream<G, D>) -> CoreStream<G, D>;
}

impl<G: Scope, D: Container+Clone+'static> Concat<G, D> for CoreStream<G, D> {
    fn concat(&self, other: &CoreStream<G, D>) -> CoreStream<G, D> {
        self.scope().concatenate(vec![self, other].into_iter().cloned())
    }
}

/// Merge the contents of multiple streams.
pub trait Concatenate<G: Scope, D: Container> {
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
    fn concatenate<I>(&self, sources: I) -> CoreStream<G, D>
    where
        I: IntoIterator<Item=CoreStream<G, D>>;
}

impl<G: Scope, D: Container+Clone+'static> Concatenate<G, D> for CoreStream<G, D> {
    fn concatenate<I>(&self, sources: I) -> CoreStream<G, D>
    where
        I: IntoIterator<Item=CoreStream<G, D>>
    {
        let clone = self.clone();
        self.scope().concatenate(Some(clone).into_iter().chain(sources))
    }
}

impl<G: Scope, D: Container+Clone+'static> Concatenate<G, D> for G
where
    D::Allocation: IntoAllocated<D>
{
    fn concatenate<I>(&self, sources: I) -> CoreStream<G, D>
    where
        I: IntoIterator<Item=CoreStream<G, D>>
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

            let mut vector = None;
            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        let data = data.assemble(&mut vector);
                        output.session(&time).give_container(data, &mut vector);
                    })
                }
            }
        });

        result
    }
}
