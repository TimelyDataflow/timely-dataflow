//! Monitor progress at a `Stream`.

use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Operate, Antichain};
use progress::frontier::MutableAntichain;
use progress::nested::subgraph::{Source, Target};
use progress::count_map::CountMap;

use dataflow::channels::pushers::Tee;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::{ParallelizationContract, Pipeline};
use dataflow::channels::pullers::Counter as PullCounter;


use Data;
use dataflow::{Stream, Scope};

/// Monitors progress at a `Stream`.
pub trait Probe<G: Scope, D: Data> {
    /// Constructs a progress probe which indicates which timestamps have elapsed at the operator.
    ///
    /// #Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Probe, Inspect};
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Configuration::Thread, |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let (mut input, probe) = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input();
    ///         let probe = stream.inspect(|x| println!("hello {:?}", x))
    ///                           .probe();
    ///         (input, probe)
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step_while(|| probe.lt(input.time()));
    ///     }
    /// }).unwrap();
    /// ```
    fn probe(&self) -> Handle<G::Timestamp>;

    /// Inserts a progress probe in a stream.
    ///
    /// #Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Probe, Inspect};
    /// use timely::dataflow::operators::probe::Handle;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Configuration::Thread, |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut probe = Handle::new();
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input();
    ///         stream.probe_with(&mut probe)
    ///               .inspect(|x| println!("hello {:?}", x));
    ///
    ///         input
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step_while(|| probe.lt(input.time()));
    ///     }
    /// }).unwrap();
    /// ```
    fn probe_with(&self, handle: &mut Handle<G::Timestamp>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Probe<G, D> for Stream<G, D> {
    fn probe(&self) -> Handle<G::Timestamp> {

        // the frontier is shared state; scope updates, handle reads.
        let handle = Handle::new();

        let mut scope = self.scope();   // clones the scope
        let channel_id = scope.new_identifier();

        let (sender, receiver) = Pipeline.connect(&mut scope, channel_id);
        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let operator = Operator {
            input: PullCounter::new(receiver),
            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            frontier: handle.frontier.clone(),
        };

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);
        Stream::new(Source { index: index, port: 0 }, registrar, scope);
        handle
    }
    fn probe_with(&self, handle: &mut Handle<G::Timestamp>) -> Stream<G, D> {

        let mut scope = self.scope();   // clones the scope
        let channel_id = scope.new_identifier();

        let (sender, receiver) = Pipeline.connect(&mut scope, channel_id);
        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let operator = Operator {
            input: PullCounter::new(receiver),
            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            frontier: handle.frontier.clone(),
        };

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);
        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }
}

/// Reports information about progress at the probe.
pub struct Handle<T:Timestamp> {
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T: Timestamp> Handle<T> {
    /// returns true iff the frontier is strictly less than `time`.
    #[inline] pub fn lt(&self, time: &T) -> bool { self.frontier.borrow().lt(time) }
    /// returns true iff the frontier is less than or equal to `time`.
    #[inline] pub fn le(&self, time: &T) -> bool { self.frontier.borrow().le(time) }
    /// returns true iff the frontier is empty.
    #[inline] pub fn done(&self) -> bool { self.frontier.borrow().elements().len() == 0 }
    /// Allocates a new handle.
    #[inline] pub fn new() -> Self { Handle { frontier: Rc::new(RefCell::new(MutableAntichain::new())) } }
}

impl<T: Timestamp> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Handle {
            frontier: self.frontier.clone()
        }
    }
}

struct Operator<T:Timestamp, D: Data> {
    input: PullCounter<T, D>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T:Timestamp, D: Data> Operate<T> for Operator<T, D> {
    fn name(&self) -> String { "Probe".to_owned() }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { 1 }

    // we need to set the initial value of the frontier
    fn set_external_summary(&mut self, _: Vec<Vec<Antichain<T::Summary>>>, counts: &mut [CountMap<T>]) {
        let mut borrow = self.frontier.borrow_mut();
        while let Some((time, delta)) = counts[0].pop() {
            borrow.update(&time, delta);
        }
    }

    // each change to the frontier should be shared
    fn push_external_progress(&mut self, counts: &mut [CountMap<T>]) {
        let mut borrow = self.frontier.borrow_mut();
        while let Some((time, delta)) = counts[0].pop() {
            borrow.update(&time, delta);
        }
    }

    // the scope does nothing. this is actually a problem, because "reachability" assumes all messages on each edge.
    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<T>], _: &mut [CountMap<T>], produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, data)) = self.input.next() {
            self.output.session(time).give_content(data);
        }

        self.output.cease();

        // extract what we know about progress from the input and output adapters.
        self.input.pull_progress(&mut consumed[0]);
        self.output.inner().pull_progress(&mut produced[0]);

        false
    }
}

#[cfg(test)]
mod tests {

    use ::Configuration;
    use ::progress::timestamp::RootTimestamp;
    use dataflow::operators::{Input, Probe};

    #[test]
    fn probe() {

        // initializes and runs a timely dataflow computation
        ::execute(Configuration::Thread, |worker| {

            // create a new input, and inspect its output
            let (mut input, probe) = worker.dataflow(move |scope| {
                let (input, stream) = scope.new_input::<String>();
                (input, stream.probe())
            });

            // introduce data and watch!
            for round in 0..10 {
                assert!(!probe.done());
                assert!(probe.le(&RootTimestamp::new(round)));
                assert!(!probe.lt(&RootTimestamp::new(round)));
                assert!(probe.lt(&RootTimestamp::new(round + 1)));
                input.advance_to(round + 1);
                worker.step();
            }

            // seal the input
            input.close();

            // finish off any remaining work
            worker.step();
            assert!(probe.done());
        }).unwrap();
    }

}
