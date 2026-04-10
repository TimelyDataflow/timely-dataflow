//! Monitor progress at a `Stream`.

use std::rc::Rc;
use std::cell::RefCell;

use crate::progress::Timestamp;
use crate::progress::frontier::{AntichainRef, MutableAntichain};
use crate::dataflow::channels::pushers::Counter as PushCounter;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::channels::pullers::Counter as PullCounter;
use crate::dataflow::operators::generic::builder_raw::OperatorBuilder;


use crate::dataflow::Stream;
use crate::Container;
use crate::dataflow::channels::Message;

/// Monitors progress at a `Stream`.
pub trait Probe<T: Timestamp, C: Container> {
    /// Constructs a progress probe which indicates which timestamps have elapsed at the operator.
    ///
    /// Returns a tuple of a probe handle and the input stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Probe, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let (mut input, probe) = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input::<Vec<_>>();
    ///         let (probe, _) = stream.inspect(|x| println!("hello {:?}", x))
    ///                                .probe();
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
    fn probe(self) -> (Handle<T>, Self);

    /// Inserts a progress probe in a stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Probe, Inspect};
    /// use timely::dataflow::operators::probe::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut probe = Handle::new();
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input::<Vec<_>>();
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
    fn probe_with(self, handle: &Handle<T>) -> Stream<T, C>;
}

impl<T: Timestamp, C: Container> Probe<T, C> for Stream<T, C> {
    fn probe(self) -> (Handle<T>, Self) {

        // the frontier is shared state; scope updates, handle reads.
        let handle = Handle::<T>::new();
        let stream = self.probe_with(&handle);
        (handle, stream)
    }
    fn probe_with(self, handle: &Handle<T>) -> Stream<T, C> {

        let mut builder = OperatorBuilder::new_from("Probe".to_owned(), Rc::clone(&self.subgraph), self.worker.clone());
        let mut input = PullCounter::new(builder.new_input(self, Pipeline));
        let (tee, stream) = builder.new_output();
        let mut output = PushCounter::new(tee);

        // Conservatively introduce a minimal time to the handle.
        // This will be relaxed when the operator is first scheduled and can see its frontier.
        handle.frontier.borrow_mut().update_iter(std::iter::once((Timestamp::minimum(), 1)));

        let shared_frontier = Rc::downgrade(&handle.frontier);
        let mut started = false;

        builder.build(
            move |progress| {

                // Mirror presented frontier changes into the shared handle.
                if let Some(shared_frontier) = shared_frontier.upgrade() {
                    let mut borrow = shared_frontier.borrow_mut();
                    borrow.update_iter(progress.frontiers[0].drain());
                }

                // At initialization, we have a few tasks.
                if !started {
                    // We must discard the capability held by `OpereratorCore`.
                    progress.internals[0].update(T::minimum(), -1);
                    // We must retract the conservative hold in the shared handle.
                    if let Some(shared_frontier) = shared_frontier.upgrade() {
                        let mut borrow = shared_frontier.borrow_mut();
                        borrow.update_iter(std::iter::once((Timestamp::minimum(), -1)));
                    }
                    started = true;
                }

                while let Some(message) = input.next() {
                    Message::push_at(&mut message.data, message.time.clone(), &mut output);
                }
                use timely_communication::Push;
                output.done();

                // extract what we know about progress from the input and output adapters.
                input.consumed().borrow_mut().drain_into(&mut progress.consumeds[0]);
                output.produced().borrow_mut().drain_into(&mut progress.produceds[0]);

                false
            },
        );

        stream
    }
}

/// Reports information about progress at the probe.
#[derive(Debug)]
pub struct Handle<T:Timestamp> {
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T: Timestamp> Handle<T> {
    /// Returns `true` iff the frontier is strictly less than `time`.
    #[inline] pub fn less_than(&self, time: &T) -> bool { self.frontier.borrow().less_than(time) }
    /// Returns `true` iff the frontier is less than or equal to `time`.
    #[inline] pub fn less_equal(&self, time: &T) -> bool { self.frontier.borrow().less_equal(time) }
    /// Returns `true` iff the frontier is empty.
    #[inline] pub fn done(&self) -> bool { self.frontier.borrow().is_empty() }
    /// Allocates a new handle.
    #[inline] pub fn new() -> Self { Handle { frontier: Rc::new(RefCell::new(MutableAntichain::new())) } }

    /// Invokes a method on the frontier, returning its result.
    ///
    /// This method allows inspection of the frontier, which cannot be returned by reference as
    /// it is on the other side of a `RefCell`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::probe::Handle;
    ///
    /// let handle = Handle::<usize>::new();
    /// let frontier = handle.with_frontier(|frontier| frontier.to_vec());
    /// ```
    #[inline]
    pub fn with_frontier<R, F: FnMut(AntichainRef<T>)->R>(&self, mut function: F) -> R {
        function(self.frontier.borrow().frontier())
    }
}

impl<T: Timestamp> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Handle {
            frontier: Rc::clone(&self.frontier)
        }
    }
}

impl<T> Default for Handle<T>
where
    T: Timestamp,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use crate::Config;
    use crate::dataflow::operators::{Input, Probe};

    #[test]
    fn probe() {

        // initializes and runs a timely dataflow computation
        crate::execute(Config::thread(), |worker| {

            // create a new input, and inspect its output
            let (mut input, probe) = worker.dataflow(move |scope| {
                let (input, stream) = scope.new_input::<Vec<String>>();
                (input, stream.probe().0)
            });

            // introduce data and watch!
            for round in 0..10 {
                assert!(!probe.done());
                assert!(probe.less_equal(&round));
                assert!(probe.less_than(&(round + 1)));
                input.advance_to(round + 1);
                worker.step();
            }

            // seal the input
            input.close();

            // finish off any remaining work
            worker.step();
            worker.step();
            worker.step();
            worker.step();
            assert!(probe.done());
        }).unwrap();
    }

}
