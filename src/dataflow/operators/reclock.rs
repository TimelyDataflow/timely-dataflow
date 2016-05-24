//! Extension methods for `Stream` based on record-by-record transformation.

use Data;
use dataflow::{Stream, Scope};
use dataflow::channels::pact::Pipeline;
use dataflow::operators::binary::Binary;

use dataflow::channels::message::Content;

/// Extension trait for `Stream`.
pub trait Reclock<S: Scope, D: Data> {
    /// Delays records until an input is observed on the `clock` input.
    ///
    /// #Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Delay, Inspect, Map, Reclock, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// // use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// let captured = timely::example(|scope| {
    ///     let data = (0..10).to_stream(scope)
    ///                       .delay(|x,t| RootTimestamp::new(*x));
    /// 
    ///     let clock = vec![3, 5, 8].into_iter()
    ///                              .to_stream(scope)
    ///                              .delay(|x,t| RootTimestamp::new(*x))
    ///                              .map(|_| ());
    ///
    ///     data.reclock(&clock)
    ///         .inspect_batch(|t,xs| println!("time: {:?}, data: {:?}", t, xs))
    ///         .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted.len(), 3);
    /// assert_eq!(extracted[0], (RootTimestamp::new(3), vec![0,1,2,3]));
    /// assert_eq!(extracted[1], (RootTimestamp::new(5), vec![4,5]));
    /// assert_eq!(extracted[2], (RootTimestamp::new(8), vec![6,7,8]));
    /// ```        
    fn reclock(&self, clock: &Stream<S, ()>) -> Stream<S, D>;
}

impl<S: Scope, D: Data> Reclock<S, D> for Stream<S, D> {
    fn reclock(&self, clock: &Stream<S, ()>) -> Stream<S, D> {

        let mut stash = vec![];

        self.binary_notify(clock, Pipeline, Pipeline, "Reclock", vec![], move |input1, input2, output, notificator| {

            while let Some((time, data)) = input1.next() {
                stash.push((time.time(), ::std::mem::replace(data, Content::Typed(vec![]))));
            }

            while let Some((time, _data)) = input2.next() {
                notificator.notify_at(time);
            }

            while let Some((cap, _)) = notificator.next() {
                let time = cap.time();
                let mut session = output.session(&cap);
                for &mut (ref t, ref mut data) in &mut stash {
                    if t.le(&time) {
                        session.give_content(data);
                    }
                }
                stash.retain(|x| !x.0.le(&time));
            }

        })

    }
}
