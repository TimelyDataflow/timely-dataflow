//! Extension methods for `Stream` based on record-by-record transformation.

use crate::Container;
use crate::progress::Timestamp;
use crate::dataflow::Stream;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for reclocking a stream.
pub trait Reclock<'scope, T: Timestamp> {
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
    /// use timely::dataflow::operators::{ToStream, Reclock, Capture};
    /// use timely::dataflow::operators::vec::{Delay, Map};
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
    ///     data.reclock(clock)
    ///         .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted.len(), 3);
    /// assert_eq!(extracted[0], (3, vec![0,1,2,3]));
    /// assert_eq!(extracted[1], (5, vec![4,5]));
    /// assert_eq!(extracted[2], (8, vec![6,7,8]));
    /// ```
    fn reclock<TC: Container>(self, clock: Stream<'scope, T, TC>) -> Self;
}

impl<'scope, T: Timestamp, C: Container> Reclock<'scope, T> for Stream<'scope, T, C> {
    fn reclock<TC: Container>(self, clock: Stream<'scope, T, TC>) -> Self {

        let mut stash = vec![];

        self.binary_notify(clock, Pipeline, Pipeline, "Reclock", vec![], move |input1, input2, output, notificator| {

            // stash each data input with its stamp; a message with no capabilities
            // could never be released, and is discarded.
            input1.for_each_stamp(|cap, data| {
                if !cap.stamp().is_empty() {
                    for data in data {
                        stash.push((cap.stamp().clone(), std::mem::take(data)));
                    }
                }
            });

            // request notification at each clock time, to flush stash.
            input2.for_each_stamp(|cap, _data| {
                for cap in cap.retain_stamp(output.output_index()).iter() {
                    notificator.notify_at(cap.clone());
                }
            });

            // each time with complete stash can be flushed: data whose stamp has an
            // element less or equal to the clock time may be sent at the clock time.
            notificator.for_each(|cap,_,_| {
                let mut session = output.session(&cap);
                for &mut (ref stamp, ref mut data) in &mut stash {
                    if stamp.less_equal(cap.time()) {
                        session.give_container(data);
                    }
                }
                stash.retain(|x| !x.0.less_equal(cap.time()));
            });
        })
    }
}
