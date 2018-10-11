//! Create cycles in a timely dataflow graph.

use std::rc::Rc;
use std::cell::RefCell;

use Data;
use communication::Push;

use progress::{Timestamp, Operate, PathSummary};
use progress::frontier::Antichain;
use progress::nested::{Source, Target};
use progress::ChangeBatch;

use progress::nested::product::Product;
// use progress::nested::Summary::Local;

use dataflow::channels::Bundle;
use dataflow::channels::pushers::{Counter, Tee};
use worker::AsWorker;

use dataflow::{Stream, Scope, ScopeParent};
use dataflow::scopes::child::Iterative;

/// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
pub trait LoopVariable<'a, G: ScopeParent, T: Timestamp> {
    /// Creates a `Stream` and a `Handle` to later bind the source of that `Stream`.
    ///
    /// The resulting `Stream` will have its data defined by a future call to `connect_loop` with
    /// its `Handle` passed as an argument. Data passed through the stream will have their
    /// timestamps advanced by `summary`, and will be dropped if the result exceeds `limit`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{LoopVariable, ConnectLoop, ToStream, Concat, Inspect};
    ///
    /// timely::example(|scope| {
    ///     scope.scoped("Loop", |scope| {
    ///         // circulate 0..10 for 100 iterations.
    ///         let (handle, cycle) = scope.loop_variable(100, 1);
    ///         (0..10).to_stream(scope)
    ///                .concat(&cycle)
    ///                .inspect(|x| println!("seen: {:?}", x))
    ///                .connect_loop(handle);
    ///     });
    /// });
    /// ```
    fn loop_variable<D: Data>(&mut self, limit: T, summary: T::Summary) -> (Handle<G::Timestamp, T, D>, Stream<Iterative<'a, G, T>, D>);
}

impl<'a, G: ScopeParent, T: Timestamp> LoopVariable<'a, G, T> for Iterative<'a, G, T> {
    fn loop_variable<D: Data>(&mut self, limit: T, summary: T::Summary) -> (Handle<G::Timestamp, T, D>, Stream<Iterative<'a, G, T>, D>) {

        let (targets, registrar) = Tee::<Product<G::Timestamp, T>, D>::new();

        let feedback_output = Counter::new(targets);
        let produced = feedback_output.produced().clone();

        let feedback_input =  Counter::new(Observer {
            limit, summary: summary.clone(), targets: feedback_output
        });
        let consumed = feedback_input.produced().clone();

        let index = self.add_operator(Box::new(Operator {
            consumed_messages:  consumed,
            produced_messages:  produced,
            summary:            Product::new(Default::default(), summary),
        }));

        let helper = Handle {
            index,
            target: feedback_input,
        };

        (helper, Stream::new(Source { index, port: 0 }, registrar, self.clone()))
    }
}

// implementation of the feedback vertex, essentially, as an observer
struct Observer<TOuter: Timestamp, TInner: Timestamp, D:Data> {
    limit:      TInner,
    summary:    TInner::Summary,
    targets:    Counter<Product<TOuter, TInner>, D, Tee<Product<TOuter, TInner>, D>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Data> Push<Bundle<Product<TOuter, TInner>, D>> for Observer<TOuter, TInner, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<Bundle<Product<TOuter, TInner>, D>>) {
        let active = if let Some(message) = message {
            let message = message.as_mut();
            if let Some(new_time) = self.summary.results_in(&message.time.inner) {
                message.time.inner = new_time;
                message.time.inner.less_equal(&self.limit)
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
pub trait ConnectLoop<G: ScopeParent, T: Timestamp, D: Data> {
    /// Connect a `Stream` to be the input of a loop variable.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{LoopVariable, ConnectLoop, ToStream, Concat, Inspect};
    ///
    /// timely::example(|scope| {
    ///     scope.scoped("Loop", |scope| {
    ///         // circulate 0..10 for 100 iterations.
    ///         let (handle, cycle) = scope.loop_variable(100, 1);
    ///         (0..10).to_stream(scope)
    ///                .concat(&cycle)
    ///                .inspect(|x| println!("seen: {:?}", x))
    ///                .connect_loop(handle);
    ///     });
    /// });
    /// ```
    fn connect_loop(&self, Handle<G::Timestamp, T, D>);
}

impl<'a, G: ScopeParent, T: Timestamp, D: Data> ConnectLoop<G, T, D> for Stream<Iterative<'a, G, T>, D> {
    fn connect_loop(&self, helper: Handle<G::Timestamp, T, D>) {
        let channel_id = self.scope().new_identifier();
        self.connect_to(Target { index: helper.index, port: 0 }, helper.target, channel_id);
    }
}

/// A handle used to bind the source of a loop variable.
pub struct Handle<TOuter: Timestamp, TInner: Timestamp, D: Data> {
    index:  usize,
    target: Counter<Product<TOuter, TInner>, D, Observer<TOuter, TInner, D>>
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
