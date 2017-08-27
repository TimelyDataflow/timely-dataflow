//! Monitor progress at a `Stream`.

use std::rc::Rc;
use std::cell::RefCell;

use progress::Timestamp;
use progress::frontier::MutableAntichain;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::Pipeline;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::operators::generic::builder_raw::OperatorBuilder;


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
    ///         worker.step_while(|| probe.less_than(input.time()));
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
    ///         worker.step_while(|| probe.less_than(input.time()));
    ///     }
    /// }).unwrap();
    /// ```
    fn probe_with(&self, handle: &mut Handle<G::Timestamp>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Probe<G, D> for Stream<G, D> {
    fn probe(&self) -> Handle<G::Timestamp> {

        // the frontier is shared state; scope updates, handle reads.
        let mut handle = Handle::<G::Timestamp>::new();
        self.probe_with(&mut handle);
        handle
    }
    fn probe_with(&self, handle: &mut Handle<G::Timestamp>) -> Stream<G, D> {

        let mut builder = OperatorBuilder::new("Probe".to_owned(), self.scope());
        let mut input = PullCounter::new(builder.new_input(self, Pipeline));
        let (tee, stream) = builder.new_output();
        let mut output = PushBuffer::new(PushCounter::new(tee));

        let frontier = handle.frontier.clone();
        let mut started = false;

        builder.build(
            move |changes| {
                frontier.borrow_mut().update_iter(changes[0].drain());
            },
            move |consumed, internal, produced| {

                if !started {
                    internal[0].update(Default::default(), -1);
                    started = true;
                }

                while let Some((time, data)) = input.next() {
                    output.session(time).give_content(data);
                }
                output.cease();

                // extract what we know about progress from the input and output adapters.
                input.consumed().borrow_mut().drain_into(&mut consumed[0]);
                output.inner().produced().borrow_mut().drain_into(&mut produced[0]);

                false
            },
        );

        stream
    }
}

/// Reports information about progress at the probe.
pub struct Handle<T:Timestamp> {
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T: Timestamp> Handle<T> {
    /// returns true iff the frontier is strictly less than `time`.
    #[inline] pub fn less_than(&self, time: &T) -> bool { self.frontier.borrow().less_than(time) }
    /// returns true iff the frontier is less than or equal to `time`.
    #[inline] pub fn less_equal(&self, time: &T) -> bool { self.frontier.borrow().less_equal(time) }
    /// returns true iff the frontier is empty.
    #[inline] pub fn done(&self) -> bool { self.frontier.borrow().is_empty() }
    /// Allocates a new handle.
    #[inline] pub fn new() -> Self { Handle { frontier: Rc::new(RefCell::new(MutableAntichain::new())) } }
    
    /// Invokes a method on the frontier, returning its result.
    ///
    /// This method allows inspection of the frontier, which cannot be returned by reference as 
    /// it is on the other side of a `RefCell`.
    ///
    /// #Examples
    ///
    /// ```
    /// use timely::dataflow::operators::probe::Handle;
    ///
    /// let handle = Handle::<usize>::new();
    /// let frontier = handle.with_frontier(|frontier| frontier.to_vec());
    /// ```
    #[inline]
    pub fn with_frontier<R, F: FnMut(&[T])->R>(&self, mut function: F) -> R {
        function(self.frontier.borrow().frontier())
    }
}

impl<T: Timestamp> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Handle {
            frontier: self.frontier.clone()
        }
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
                assert!(probe.less_equal(&RootTimestamp::new(round)));
                assert!(!probe.less_than(&RootTimestamp::new(round)));
                assert!(probe.less_than(&RootTimestamp::new(round + 1)));
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
