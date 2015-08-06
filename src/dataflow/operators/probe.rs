//! An extension method to monitor progress at a `Stream`.

use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Operate, Antichain};
use progress::frontier::MutableAntichain;
use progress::nested::subgraph::Source::ChildOutput;
use progress::nested::subgraph::Target::ChildInput;
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
    fn probe(&self) -> (Handle<G::Timestamp>, Stream<G, D>);
}

impl<G: Scope, D: Data> Probe<G, D> for Stream<G, D> {
    fn probe(&self) -> (Handle<G::Timestamp>, Stream<G, D>) {

        // the frontier is shared state; scope updates, handle reads.
        let frontier = Rc::new(RefCell::new(MutableAntichain::new()));
        let handle = Handle { frontier: frontier.clone() };

        let mut scope = self.scope();   // clones the scope

        let (sender, receiver) = Pipeline.connect(&mut scope);
        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let operator = Operator {
            input: PullCounter::new(receiver),
            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            frontier: frontier,
        };

        let index = scope.add_operator(operator);
        self.connect_to(ChildInput(index, 0), sender);
        (handle, Stream::new(ChildOutput(index, 0), registrar, scope))
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
}

struct Operator<T:Timestamp, D: Data> {
    input: PullCounter<T, D>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T:Timestamp, D: Data> Operate<T> for Operator<T, D> {
    fn name(&self) -> &str { "Probe" }
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
    fn pull_internal_progress(&mut self,_: &mut [CountMap<T>], consumed: &mut [CountMap<T>], produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, data)) = self.input.next() {
            self.output.session(time).give_content(data);
        }

        self.output.cease();

        // extract what we know about progress from the input and output adapters.
        self.input.pull_progress(&mut consumed[0]);
        self.output.inner().pull_progress(&mut produced[0]);

        false
    }

    fn notify_me(&self) -> bool { true }
}

#[cfg(test)]
mod tests {

    use ::Configuration;
    use ::progress::timestamp::RootTimestamp;
    use dataflow::*;
    use dataflow::operators::{Input, Probe};

    #[test]
    fn probe() {

        // initializes and runs a timely dataflow computation
        ::execute(Configuration::Thread, |computation| {

            // create a new input, and inspect its output
            let (mut input, probe) = computation.scoped(move |builder| {
                let (input, stream) = builder.new_input::<String>();
                (input, stream.probe().0)
            });

            // introduce data and watch!
            for round in 0..10 {
                assert!(!probe.done());
                assert!(probe.le(&RootTimestamp::new(round)));
                assert!(!probe.lt(&RootTimestamp::new(round)));
                assert!(probe.lt(&RootTimestamp::new(round + 1)));
                input.advance_to(round + 1);
                computation.step();
            }

            // seal the input
            input.close();

            // finish off any remaining work
            computation.step();
            assert!(probe.done());
        });
    }

}
