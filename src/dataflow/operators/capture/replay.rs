//! Traits and types for replaying captured timely dataflow streams.
//!
//! A type can be replayed into any timely dataflow scope if it presents as an
//! iterator whose `Item` type implements `EventIterator` with the same timestamp. 
//! Other types can implement the `ReplayInto` trait, but this should be done with
//! care, as there is a protocol the replayer follows that must be respected if the
//! computation is to make sense.
//!
//! #Protocol
//! 
//! The stream of events produced by each `EventIterator` implementation must satisfy, 
//! starting from a default timestamp of `Default::default()` with count 1,
//!
//! 1. The progress messages may only increment the count for a timestamp if
//!    the cumulative count for some prior or equal timestamp is positive.
//! 2. The data messages map only use a timestamp if the cumulative count for 
//!    some prior or equal timestamp is positive.
//!
//! Alternately, the sequence of events should, starting from an initial count of 1 
//! for the timestamp `Default::default()`, describe decrements to held capabilities
//! or the production of capabilities in their future, or messages sent at times in 
//! the future of held capabilities.
//!
//! The order is very important here. One can move `Event::Message` events arbitrarily
//! earlier in the sequence, and `Event::Progress` events arbitrarily later, but one
//! cannot move a `Event::Progress` message that discards a last capability before any
//! `Event::Message` that would use that capability.
//!
//! For an example, the `Operate<T>` implementation for `capture::CaptureOperator<T, D, P>`
//! records exactly what data is presented at the operator, both in terms of progress
//! messages and data received.
//!
//! #Notes
//! 
//! Provided no stream of events reports the consumption of capabilities it does not hold, 
//! any interleaving of the streams of events will still maintain the invariants above.
//! This means that each timely dataflow replay operator can replay any number of streams,
//! allowing the replay to occur in a timely dataflow computation with more or fewer workers
//! than that in which the stream was captured.

use ::Data;
use dataflow::{Scope, Stream};
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::operators::generic::builder_raw::OperatorBuilder;
use progress::Timestamp;

use super::Event;
use super::event::EventIterator;

/// Replay a capture stream into a scope with the same timestamp.
pub trait Replay<T: Timestamp, D: Data> {
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    fn replay_into<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>;
}

impl<T: Timestamp, D: Data, I> Replay<T, D> for I
where I : IntoIterator,
      <I as IntoIterator>::Item: EventIterator<T, D>+'static {
    fn replay_into<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>{

        let mut builder = OperatorBuilder::new("Replay".to_owned(), scope.clone());
        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        builder.build(
            move |_frontier| { }, 
            move |_consumed, internal, produced| {

                if !started {
                    // The first thing we do is modify our capabilities to match the number of streams we manage.
                    // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                    // our very first action.
                    internal[0].update(Default::default(), (event_streams.len() as i64) - 1);
                    started = true;
                }

                for event_stream in event_streams.iter_mut() {
                    while let Some(event) = event_stream.next() {
                        match *event {
                            Event::Progress(ref vec) => {
                                internal[0].extend(vec.iter().cloned());
                            },
                            Event::Messages(ref time, ref data) => {
                                output.session(time).give_iterator(data.iter().cloned());
                            }
                        }
                    }
                }

                output.cease();
                output.inner().produced().borrow_mut().drain_into(&mut produced[0]);

                false
            }
        );

        stream
    }
}