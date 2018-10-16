//! Create cycles in a timely dataflow graph.

use std::rc::Rc;
use std::cell::RefCell;

use Data;
use communication::Push;

use progress::{Timestamp, Operate, PathSummary};
use progress::frontier::Antichain;
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use progress::ChangeBatch;

use dataflow::channels::Bundle;
use dataflow::channels::pushers::{Counter, Tee};
use dataflow::{Stream, Scope};
use dataflow::scopes::child::Iterative;

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
    fn feedback<D: Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G::Timestamp, D>, Stream<G, D>);
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
    fn loop_variable<D: Data>(&mut self, summary: T::Summary) -> (Handle<Product<G::Timestamp, T>, D>, Stream<Iterative<'a, G, T>, D>);
}

impl<G: Scope> Feedback<G> for G {
    fn feedback<D: Data>(&mut self, summary: <G::Timestamp as Timestamp>::Summary) -> (Handle<G::Timestamp, D>, Stream<G, D>) {

        if summary == Default::default() {
            panic!("Cannot use default summary for a loop variable");
        }

        let (targets, registrar) = Tee::<G::Timestamp, D>::new();

        let feedback_output = Counter::new(targets);
        let produced_messages = feedback_output.produced().clone();

        let feedback_input =  Counter::new(Observer {
            summary: summary.clone(), targets: feedback_output
        });
        let consumed_messages = feedback_input.produced().clone();

        let index = self.add_operator(Box::new(Operator {
            consumed_messages,
            produced_messages,
            summary,
        }));

        let helper = Handle {
            index,
            target: feedback_input,
        };

        (helper, Stream::new(Source { index, port: 0 }, registrar, self.clone()))
    }
}

impl<'a, G: Scope, T: Timestamp> LoopVariable<'a, G, T> for Iterative<'a, G, T> {
    fn loop_variable<D: Data>(&mut self, summary: T::Summary) -> (Handle<Product<G::Timestamp, T>, D>, Stream<Iterative<'a, G, T>, D>) {
        self.feedback(Product::new(Default::default(), summary))
    }
}

// implementation of the feedback vertex, essentially, as an observer
struct Observer<T: Timestamp, D:Data> {
    summary:    T::Summary,
    targets:    Counter<T, D, Tee<T, D>>,
}

impl<T: Timestamp, D: Data> Push<Bundle<T, D>> for Observer<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        // We propagate the message either if its time results in something valid,
        // or in the case that `message` is `None`. Note: we do not push a `None`
        // when the timestamp is invalid, as we are not yet meant to flush.
        let active = if let Some(message) = message {
            let message = message.as_mut();
            if let Some(new_time) = self.summary.results_in(&message.time) {
                message.time = new_time;
                true
            }
            else {
                false
            }
        }
        else { true };

        if active { self.targets.push(message); }
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
    fn connect_loop(&self, Handle<G::Timestamp, D>);
}

impl<G: Scope, D: Data> ConnectLoop<G, D> for Stream<G, D> {
    fn connect_loop(&self, helper: Handle<G::Timestamp, D>) {
        let channel_id = self.scope().new_identifier();
        self.connect_to(Target { index: helper.index, port: 0 }, helper.target, channel_id);
    }
}

/// A handle used to bind the source of a loop variable.
pub struct Handle<T: Timestamp, D: Data> {
    index:  usize,
    target: Counter<T, D, Observer<T, D>>
}

// the scope that the progress tracker interacts with
struct Operator<T:Timestamp> {
    consumed_messages:  Rc<RefCell<ChangeBatch<T>>>,
    produced_messages:  Rc<RefCell<ChangeBatch<T>>>,
    summary:            T::Summary,
}


impl<T:Timestamp> Operate<T> for Operator<T> {
    fn name(&self) -> String { "Feedback".to_owned() }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {
        (vec![vec![Antichain::from_elem(self.summary.clone())]], vec![ChangeBatch::new()])
    }

    fn pull_internal_progress(&mut self, messages_consumed: &mut [ChangeBatch<T>],
                                        _frontier_progress: &mut [ChangeBatch<T>],
                                         messages_produced: &mut [ChangeBatch<T>]) -> bool {

        self.consumed_messages.borrow_mut().drain_into(&mut messages_consumed[0]);
        self.produced_messages.borrow_mut().drain_into(&mut messages_produced[0]);
        false
    }

    fn notify_me(&self) -> bool { false }
}
