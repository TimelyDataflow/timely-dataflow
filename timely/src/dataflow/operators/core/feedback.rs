//! Create cycles in a timely dataflow graph.

use crate::Container;

use crate::progress::{Timestamp, PathSummary};
use crate::progress::frontier::Antichain;
use crate::order::Product;

use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{StreamCore, Scope};
use crate::dataflow::scopes::child::Iterative;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::operators::generic::OutputWrapper;

/// Creates a `StreamCore` and a `Handle` to later bind the source of that `StreamCore`.
pub trait Feedback<G: Scope> {

    /// Creates a [StreamCore] and a [Handle] to later bind the source of that `StreamCore`.
    ///
    /// The resulting `StreamCore` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Containers passed through the stream will have their
    /// timestamps advanced by `summary`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Feedback, ConnectLoop, ToStream, Concat, Inspect, BranchWhen};
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     let (handle, cycle) = scope.feedback(1);
    ///     (0..10).to_stream(scope)
    ///            .concat(&cycle)
    ///            .inspect(|x| println!("seen: {:?}", x))
    ///            .branch_when(|t| t < &100).1
    ///            .connect_loop(handle);
    /// });
    /// ```
    fn feedback<C: Container>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, C>, StreamCore<G, C>);
}

/// Creates a `StreamCore` and a `Handle` to later bind the source of that `StreamCore`.
pub trait LoopVariable<'a, G: Scope, T: Timestamp> {
    /// Creates a `StreamCore` and a `Handle` to later bind the source of that `StreamCore`.
    ///
    /// The resulting `StreamCore` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Containers passed through the stream will have their
    /// timestamps advanced by `summary`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{LoopVariable, ConnectLoop, ToStream, Concat, Inspect, BranchWhen};
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     scope.iterative::<usize,_,_>(|inner| {
    ///         let (handle, cycle) = inner.loop_variable(1);
    ///         (0..10).to_stream(inner)
    ///                .concat(&cycle)
    ///                .inspect(|x| println!("seen: {:?}", x))
    ///                .branch_when(|t| t.inner < 100).1
    ///                .connect_loop(handle);
    ///     });
    /// });
    /// ```
    fn loop_variable<C: Container>(&mut self, summary: T::Summary) -> (Handle<Iterative<'a, G, T>, C>, StreamCore<Iterative<'a, G, T>, C>);
}

impl<G: Scope> Feedback<G> for G {

    fn feedback<C: Container>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, C>, StreamCore<G, C>) {

        let mut builder = OperatorBuilder::new("Feedback".to_owned(), self.clone());
        let (output, stream) = builder.new_output();

        (Handle { builder, summary, output }, stream)
    }
}

impl<'a, G: Scope, T: Timestamp> LoopVariable<'a, G, T> for Iterative<'a, G, T> {
    fn loop_variable<C: Container>(&mut self, summary: T::Summary) -> (Handle<Iterative<'a, G, T>, C>, StreamCore<Iterative<'a, G, T>, C>) {
        self.feedback(Product::new(Default::default(), summary))
    }
}

/// Connect a `Stream` to the input of a loop variable.
pub trait ConnectLoop<G: Scope, C: Container> {
    /// Connect a `Stream` to be the input of a loop variable.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Feedback, ConnectLoop, ToStream, Concat, Inspect, BranchWhen};
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     let (handle, cycle) = scope.feedback(1);
    ///     (0..10).to_stream(scope)
    ///            .concat(&cycle)
    ///            .inspect(|x| println!("seen: {:?}", x))
    ///            .branch_when(|t| t < &100).1
    ///            .connect_loop(handle);
    /// });
    /// ```
    fn connect_loop(&self, handle: Handle<G, C>);
}

impl<G: Scope, C: Container> ConnectLoop<G, C> for StreamCore<G, C> {
    fn connect_loop(&self, handle: Handle<G, C>) {

        let mut builder = handle.builder;
        let summary = handle.summary;
        let mut output = handle.output;

        let mut input = builder.new_input_connection(self, Pipeline, vec![Antichain::from_elem(summary.clone())]);

        let mut vector = Default::default();
        builder.build(move |_capability| move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                data.swap(&mut vector);
                if let Some(new_time) = summary.results_in(cap.time()) {
                    let new_cap = cap.delayed(&new_time);
                    output
                        .session(&new_cap)
                        .give_container(&mut vector);
                }
            });
        });
    }
}

/// A handle used to bind the source of a loop variable.
#[derive(Debug)]
pub struct Handle<G: Scope, C: Container> {
    builder: OperatorBuilder<G>,
    summary: <G::Timestamp as Timestamp>::Summary,
    output: OutputWrapper<G::Timestamp, C, Tee<G::Timestamp, C>>,
}
