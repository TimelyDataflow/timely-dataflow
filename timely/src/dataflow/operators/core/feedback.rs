//! Create cycles in a timely dataflow graph.

use crate::Container;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::scope::Iterative;
use crate::dataflow::{Stream, Scope};
use crate::order::Product;
use crate::progress::frontier::Antichain;
use crate::progress::{Timestamp, PathSummary};

/// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
pub trait Feedback<'scope, T: Timestamp> {

    /// Creates a [Stream] and a [Handle] to later bind the source of that `Stream`.
    ///
    /// The resulting `Stream` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Containers passed through the stream will have their
    /// timestamps advanced by `summary`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Feedback, ConnectLoop, ToStream, Concat, Inspect};
    /// use timely::dataflow::operators::vec::BranchWhen;
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     let (handle, cycle) = scope.feedback(1);
    ///     (0..10).to_stream(scope)
    ///            .container::<Vec<_>>()
    ///            .concat(cycle)
    ///            .inspect(|x| println!("seen: {:?}", x))
    ///            .branch_when(|t| t < &100).1
    ///            .connect_loop(handle);
    /// });
    /// ```
    fn feedback<C: Container>(&self, summary: <T as Timestamp>::Summary) -> (Handle<'scope, T, C>, Stream<'scope, T, C>);
}

/// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
pub trait LoopVariable<'scope, TOuter: Timestamp, TInner: Timestamp> {
    /// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
    ///
    /// The resulting `Stream` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Containers passed through the stream will have their
    /// timestamps advanced by `summary`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{LoopVariable, ConnectLoop, ToStream, Concat, Inspect};
    /// use timely::dataflow::operators::vec::BranchWhen;
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     scope.iterative::<usize,_,_>(|inner| {
    ///         let (handle, cycle) = inner.loop_variable(1);
    ///         (0..10).to_stream(inner)
    ///                .container::<Vec<_>>()
    ///                .concat(cycle)
    ///                .inspect(|x| println!("seen: {:?}", x))
    ///                .branch_when(|t| t.inner < 100).1
    ///                .connect_loop(handle);
    ///     });
    /// });
    /// ```
    fn loop_variable<C: Container>(&self, summary: TInner::Summary) -> (Handle<'scope, Product<TOuter, TInner>, C>, Stream<'scope, Product<TOuter, TInner>, C>);
}

impl<'scope, T: Timestamp> Feedback<'scope, T> for Scope<'scope, T> {

    fn feedback<C: Container>(&self, summary: <T as Timestamp>::Summary) -> (Handle<'scope, T, C>, Stream<'scope, T, C>) {

        let mut builder = OperatorBuilder::new("Feedback".to_owned(), *self);
        let (output, stream) = builder.new_output();

        (Handle { builder, summary, output }, stream)
    }
}

impl<'scope, TOuter: Timestamp, TInner: Timestamp> LoopVariable<'scope, TOuter, TInner> for Iterative<'scope, TOuter, TInner> {
    fn loop_variable<C: Container>(&self, summary: TInner::Summary) -> (Handle<'scope, Product<TOuter, TInner>, C>, Stream<'scope, Product<TOuter, TInner>, C>) {
        self.feedback(Product::new(Default::default(), summary))
    }
}

/// Connect a `Stream` to the input of a loop variable.
pub trait ConnectLoop<'scope, T: Timestamp, C: Container> {
    /// Connect a `Stream` to be the input of a loop variable.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Feedback, ConnectLoop, ToStream, Concat, Inspect};
    /// use timely::dataflow::operators::vec::BranchWhen;
    ///
    /// timely::example(|scope| {
    ///     // circulate 0..10 for 100 iterations.
    ///     let (handle, cycle) = scope.feedback(1);
    ///     (0..10).to_stream(scope)
    ///            .container::<Vec<_>>()
    ///            .concat(cycle)
    ///            .inspect(|x| println!("seen: {:?}", x))
    ///            .branch_when(|t| t < &100).1
    ///            .connect_loop(handle);
    /// });
    /// ```
    fn connect_loop(self, handle: Handle<'scope, T, C>);
}

impl<'scope, T: Timestamp, C: Container> ConnectLoop<'scope, T, C> for Stream<'scope, T, C> {
    fn connect_loop(self, handle: Handle<'scope, T, C>) {

        let mut builder = handle.builder;
        let summary = handle.summary;
        let mut output = handle.output;

        let mut input = builder.new_input_connection(self, Pipeline, [(0, Antichain::from_elem(summary.clone()))]);
        builder.set_notify_for(0, crate::progress::operate::FrontierInterest::Never);

        builder.build(move |_capability| move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                // Advance each stamp element by the summary, discarding elements that
                // cannot traverse the feedback edge and restoring minimality. Contents
                // at times supported only by discarded elements cannot be sent
                // downstream, just as a message with a singleton stamp is discarded
                // entirely when its element cannot traverse.
                let new_caps = cap.stamp()
                    .map_into(|time| summary.results_in(time))
                    .iter()
                    .map(|time| cap.delayed(time, output.output_index()))
                    .collect::<crate::dataflow::operators::CapabilitySet<_>>();
                if !new_caps.is_empty() || cap.stamp().is_empty() {
                    output.give(&new_caps, data);
                }
            });
        });
    }
}

/// A handle used to bind the source of a loop variable.
#[derive(Debug)]
pub struct Handle<'scope, T: Timestamp, C: Container> {
    builder: OperatorBuilder<'scope, T>,
    summary: <T as Timestamp>::Summary,
    output: crate::dataflow::channels::pushers::Output<T, C>,
}
