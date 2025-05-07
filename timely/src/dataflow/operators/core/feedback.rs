//! Create cycles in a timely dataflow graph.

use crate::{Container, Data};
use crate::container::CapacityContainerBuilder;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::operators::generic::OutputWrapper;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::scopes::child::Iterative;
use crate::dataflow::{StreamCore, Scope};
use crate::order::Product;
use crate::progress::frontier::Antichain;
use crate::progress::{Timestamp, PathSummary};
use crate::progress::subgraph::SubgraphBuilderT;

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
    fn feedback<C: Container + Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, C>, StreamCore<G, C>);
}

/// Creates a `StreamCore` and a `Handle` to later bind the source of that `StreamCore`.
pub trait LoopVariable<'a, G, T, SG>
where
    G: Scope,
    T: Timestamp,
    SG: SubgraphBuilderT<G::Timestamp, Product<G::Timestamp, T>>,
{
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
    /// use timely::progress::SubgraphBuilder;
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     scope.iterative::<usize,_,_,SubgraphBuilder<_,_>>(|inner| {
    ///         let (handle, cycle) = inner.loop_variable(1);
    ///         (0..10).to_stream(inner)
    ///                .concat(&cycle)
    ///                .inspect(|x| println!("seen: {:?}", x))
    ///                .branch_when(|t| t.inner < 100).1
    ///                .connect_loop(handle);
    ///     });
    /// });
    /// ```
    fn loop_variable<C: Container + Data>(&mut self, summary: T::Summary) -> (Handle<Iterative<'a, G, T, SG>, C>, StreamCore<Iterative<'a, G, T, SG>, C>);
}

impl<G: Scope> Feedback<G> for G {

    fn feedback<C: Container + Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G, C>, StreamCore<G, C>) {

        let mut builder = OperatorBuilder::new("Feedback".to_owned(), self.clone());
        let (output, stream) = builder.new_output();

        (Handle { builder, summary, output }, stream)
    }
}

impl<'a, G, T, SG> LoopVariable<'a, G, T, SG> for Iterative<'a, G, T, SG>
where
    G: Scope,
    T: Timestamp,
    SG: SubgraphBuilderT<G::Timestamp, Product<G::Timestamp, T>>,
{
    fn loop_variable<C: Container + Data>(&mut self, summary: T::Summary) -> (Handle<Iterative<'a, G, T, SG>, C>, StreamCore<Iterative<'a, G, T, SG>, C>) {
        self.feedback(Product::new(Default::default(), summary))
    }
}

/// Connect a `Stream` to the input of a loop variable.
pub trait ConnectLoop<G: Scope, C: Container + Data> {
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

impl<G: Scope, C: Container + Data> ConnectLoop<G, C> for StreamCore<G, C> {
    fn connect_loop(&self, handle: Handle<G, C>) {

        let mut builder = handle.builder;
        let summary = handle.summary;
        let mut output = handle.output;

        let mut input = builder.new_input_connection(self, Pipeline, [(0, Antichain::from_elem(summary.clone()))]);

        builder.build(move |_capability| move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                if let Some(new_time) = summary.results_in(cap.time()) {
                    let new_cap = cap.delayed(&new_time);
                    output
                        .session(&new_cap)
                        .give_container(data);
                }
            });
        });
    }
}

/// A handle used to bind the source of a loop variable.
#[derive(Debug)]
pub struct Handle<G: Scope, C: Container + Data> {
    builder: OperatorBuilder<G>,
    summary: <G::Timestamp as Timestamp>::Summary,
    output: OutputWrapper<G::Timestamp, CapacityContainerBuilder<C>, Tee<G::Timestamp, C>>,
}
