//! Extension methods for `Stream` based on record-by-record transformation.

use crate::Container;
use crate::order::PartialOrder;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for reclocking a stream.
pub trait Reclock<S: Scope> {
    /// Delays records until an input is observed on the `clock` input.
    ///
    /// The source stream is buffered until a record is seen on the clock input,
    /// at which point a notification is requested and all data with time less
    /// or equal to the clock time are sent. This method does not ensure that all
    /// workers receive the same clock records, which can be accomplished with
    /// `broadcast`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Map, Reclock, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let captured = timely::example(|scope| {
    ///
    ///     // produce data 0..10 at times 0..10.
    ///     let data = (0..10).to_stream(scope)
    ///                       .delay(|x,t| *x);
    ///
    ///     // product clock ticks at three times.
    ///     let clock = vec![3, 5, 8].into_iter()
    ///                              .to_stream(scope)
    ///                              .delay(|x,t| *x)
    ///                              .map(|_| ());
    ///
    ///     // reclock the data.
    ///     data.reclock(&clock)
    ///         .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted.len(), 3);
    /// assert_eq!(extracted[0], (3, vec![0,1,2,3]));
    /// assert_eq!(extracted[1], (5, vec![4,5]));
    /// assert_eq!(extracted[2], (8, vec![6,7,8]));
    /// ```
    fn reclock<TC: Container<Item=()>>(&self, clock: &StreamCore<S, TC>) -> Self;
}

impl<S: Scope, C: Container> Reclock<S> for StreamCore<S, C> {
    fn reclock<TC: Container<Item=()>>(&self, clock: &StreamCore<S, TC>) -> StreamCore<S, C> {

        let mut stash = vec![];

        self.binary_notify(clock, Pipeline, Pipeline, "Reclock", vec![], move |input1, input2, output, notificator| {

            // stash each data input with its timestamp.
            while let Some((cap, data)) = input1.next() {
                stash.push((cap.time().clone(), data.take()));
            };

            // request notification at time, to flush stash.
            while let Some((time, _data)) = input2.next() {
                notificator.notify_at(time.retain());
            };

            // each time with complete stash can be flushed.
            notificator.for_each(|cap,_,_| {
                let mut session = output.session(&cap);
                for &mut (ref t, ref mut data) in &mut stash {
                    if t.less_equal(cap.time()) {
                        session.give_container(data);
                    }
                }
                stash.retain(|x| !x.0.less_equal(cap.time()));
            });
        })
    }
}
