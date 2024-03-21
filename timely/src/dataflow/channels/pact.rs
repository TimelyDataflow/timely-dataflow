//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: AsWorker`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::{fmt::{self, Debug}, marker::PhantomData};

use crate::Container;
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::communication::{Push, Pull, Data};
use crate::container::PushPartitioned;
use crate::dataflow::channels::pushers::Exchange as ExchangePusher;
use crate::dataflow::channels::{Bundle, Message};
use crate::logging::{TimelyLogger as Logger, MessagesEvent};
use crate::progress::Timestamp;
use crate::worker::AsWorker;

/// A `ParallelizationContract` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContract<T, C> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<Bundle<T, C>>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<Bundle<T, C>>+'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller);
}

/// A direct connection
#[derive(Debug)]
pub struct Pipeline;

impl<T: 'static, C: Container> ParallelizationContract<T, C> for Pipeline {
    type Pusher = LogPusher<T, C, ThreadPusher<Bundle<T, C>>>;
    type Puller = LogPuller<T, C, ThreadPuller<Bundle<T, C>>>;
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (pusher, puller) = allocator.pipeline::<Message<T, C>>(identifier, address);
        (LogPusher::new(pusher, allocator.index(), allocator.index(), identifier, logging.clone()),
         LogPuller::new(puller, allocator.index(), identifier, logging))
    }
}

/// An exchange between multiple observers by data
pub struct ExchangeCore<C, F> { hash_func: F, phantom: PhantomData<C> }

/// [ExchangeCore] specialized to vector-based containers.
pub type Exchange<D, F> = ExchangeCore<Vec<D>, F>;

impl<C, F> ExchangeCore<C, F>
where
    C: PushPartitioned,
    for<'a> F: FnMut(&C::Item<'a>)->u64
{
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> ExchangeCore<C, F> {
        ExchangeCore {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Timestamp, C, H: 'static> ParallelizationContract<T, C> for ExchangeCore<C, H>
where
    C: Data + PushPartitioned,
    for<'a> H: FnMut(&C::Item<'a>) -> u64
{
    type Pusher = ExchangePusher<T, C, LogPusher<T, C, Box<dyn Push<Bundle<T, C>>>>, H>;
    type Puller = LogPuller<T, C, Box<dyn Pull<Bundle<T, C>>>>;

    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, C>>(identifier, address);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        (ExchangePusher::new(senders, self.hash_func), LogPuller::new(receiver, allocator.index(), identifier, logging.clone()))
    }
}

impl<C, F> Debug for ExchangeCore<C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchange").finish()
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPusher<T, C, P: Push<Bundle<T, C>>> {
    pusher: P,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
    phantom: PhantomData<(T, C)>,
    logging: Option<Logger>,
}

impl<T, C, P: Push<Bundle<T, C>>> LogPusher<T, C, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, source: usize, target: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPusher {
            pusher,
            channel,
            counter: 0,
            source,
            target,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, C: Container, P: Push<Bundle<T, C>>> Push<Bundle<T, C>> for LogPusher<T, C, P> {
    #[inline]
    fn push(&mut self, pair: &mut Option<Bundle<T, C>>) {
        if let Some(bundle) = pair {
            self.counter += 1;

            // Stamp the sequence number and source.
            // FIXME: Awkward moment/logic.
            if let Some(message) = bundle.if_mut() {
                message.seq = self.counter - 1;
                message.from = self.source;
            }

            if let Some(logger) = self.logging.as_ref() {
                logger.log(MessagesEvent {
                    is_send: true,
                    channel: self.channel,
                    source: self.source,
                    target: self.target,
                    seq_no: self.counter - 1,
                    length: bundle.data.len(),
                })
            }
        }

        self.pusher.push(pair);
    }
}

/// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPuller<T, C, P: Pull<Bundle<T, C>>> {
    puller: P,
    channel: usize,
    index: usize,
    phantom: PhantomData<(T, C)>,
    logging: Option<Logger>,
}

impl<T, C, P: Pull<Bundle<T, C>>> LogPuller<T, C, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPuller {
            puller,
            channel,
            index,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, C: Container, P: Pull<Bundle<T, C>>> Pull<Bundle<T, C>> for LogPuller<T, C, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<Bundle<T, C>> {
        let result = self.puller.pull();
        if let Some(bundle) = result {
            let channel = self.channel;
            let target = self.index;

            if let Some(logger) = self.logging.as_ref() {
                logger.log(MessagesEvent {
                    is_send: false,
                    channel,
                    source: bundle.from,
                    target,
                    seq_no: bundle.seq,
                    length: bundle.data.len(),
                });
            }
        }

        result
    }
}
