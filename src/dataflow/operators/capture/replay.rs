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

use std::rc::Rc;
use std::cell::RefCell;

use ::Data;
use dataflow::{Scope, Stream};
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pushers::Tee;

use progress::ChangeBatch;
use progress::nested::subgraph::Source;
use progress::{Timestamp, Operate, Antichain};

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
       let (targets, registrar) = Tee::<S::Timestamp, D>::new();
       let operator = ReplayOperator {
           peers: scope.peers(),
           started: false,
           output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(ChangeBatch::new())))),
           event_streams: self.into_iter().collect(),
       };

       let index = scope.add_operator(operator);
       Stream::new(Source { index: index, port: 0 }, registrar, scope.clone())
   }
}


struct ReplayOperator<T:Timestamp, D: Data, I: EventIterator<T, D>> {
    peers: usize,
    started: bool,
    event_streams: Vec<I>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
}

impl<T:Timestamp, D: Data, I: EventIterator<T, D>> Operate<T> for ReplayOperator<T, D, I> {
    fn name(&self) -> String { "Replay".to_owned() }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {

        // This initial configuration assumes the worst about the stream, but the first progress indication
        // will amend initial capabilities. In principle, we could try and read some first few event messages,
        // but we want to avoid blocking graph construction.

        let mut result = ChangeBatch::new();
        for _ in 0 .. self.peers {
            result.update(Default::default(), 1);
        }

        (vec![], vec![result])
    }

    fn pull_internal_progress(&mut self, _: &mut [ChangeBatch<T>], internal: &mut [ChangeBatch<T>], produced: &mut [ChangeBatch<T>]) -> bool {

        if !self.started {
            // The first thing we do is modify our capabilities to match the number of streams we manage.
            // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
            // our very first action.
            internal[0].update(Default::default(), (self.event_streams.len() as i64) - 1);
            self.started = true;
        }

        for event_stream in self.event_streams.iter_mut() {
            while let Some(event) = event_stream.next() {
                match *event {
                    Event::Start => { },
                    Event::Progress(ref vec) => {
                        internal[0].extend(vec.iter().cloned());
                    },
                    Event::Messages(ref time, ref data) => {
                        self.output.session(time).give_iterator(data.iter().cloned());
                    }
                }
            }
        }

        self.output.cease();
        self.output.inner().pull_progress(&mut produced[0]);

        false
    }
}
