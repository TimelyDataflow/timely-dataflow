//! Create cycles in a timely dataflow graph.

use crate::Data;

use crate::progress::{Timestamp, PathSummary};
use crate::progress::frontier::Antichain;
use crate::order::Product;

use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::scopes::child::Iterative;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::operators::generic::OutputWrapper;

/// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
pub trait Feedback<G: Scope> {
    /// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
    ///
    /// The resulting `Stream` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Data passed through the stream will have their
    /// timestamps advanced by `summary`, and will be dropped if the result exceeds `limit`.
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
    fn feedback<D: Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, D>, Stream<G, D>);
}

/// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
pub trait LoopVariable<'a, G: Scope, T: Timestamp> {
    /// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
    ///
    /// The resulting `Stream` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Data passed through the stream will have their
    /// timestamps advanced by `summary`, and will be dropped if the result exceeds `limit`.
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
    fn loop_variable<D: Data>(&mut self, summary: T::Summary) -> (Handle<Iterative<'a, G, T>, D>, Stream<Iterative<'a, G, T>, D>);
}

impl<G: Scope> Feedback<G> for G {
    fn feedback<D: Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, D>, Stream<G, D>) {

        let mut builder = OperatorBuilder::new("Feedback".to_owned(), self.clone());
        let (output, stream) = builder.new_output();

        (Handle { builder, summary, output }, stream)
    }
}

impl<'a, G: Scope, T: Timestamp> LoopVariable<'a, G, T> for Iterative<'a, G, T> {
    fn loop_variable<D: Data>(&mut self, summary: T::Summary) -> (Handle<Iterative<'a, G, T>, D>, Stream<Iterative<'a, G, T>, D>) {
        self.feedback(Product::new(Default::default(), summary))
    }
}

/// Connect a `Stream` to the input of a loop variable.
pub trait ConnectLoop<G: Scope, D: Data> {
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
    fn connect_loop(&self, _: Handle<G, D>);
}

impl<G: Scope, D: Data> ConnectLoop<G, D> for Stream<G, D> {
    fn connect_loop(&self, helper: Handle<G, D>) {

        let mut builder = helper.builder;
        let summary = helper.summary;
        let mut output = helper.output;

        let mut input = builder.new_input_connection(self, Pipeline, vec![Antichain::from_elem(summary.clone())]);

        let mut vector = Vec::new();
        builder.build(move |_capability| move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                data.swap(&mut vector);
                if let Some(new_time) = summary.results_in(cap.time()) {
                    let new_cap = cap.delayed(&new_time);
                    output
                        .session(&new_cap)
                        .give_vec(&mut vector);
                }
            });
        });
    }
}

/// A handle used to bind the source of a loop variable.
pub struct Handle<G: Scope, D: Data> {
    builder: OperatorBuilder<G>,
    summary: <G::Timestamp as Timestamp>::Summary,
    output: OutputWrapper<G::Timestamp, D, Tee<G::Timestamp, D>>,
}
