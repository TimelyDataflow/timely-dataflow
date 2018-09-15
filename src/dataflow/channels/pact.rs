//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: Allocate`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::marker::PhantomData;

use communication::{Allocate, Push, Pull, Data};
use communication::allocator::Thread;
use communication::allocator::thread::Pusher as ThreadPusher;
use communication::allocator::thread::Puller as ThreadPuller;

use dataflow::channels::pushers::Exchange as ExchangePusher;
use super::{Bundle, Message};

use logging::TimelyLogger as Logger;

use abomonation::Abomonation;

/// A `ParallelizationContract` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContract<T: 'static, D: 'static> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<Bundle<T, D>>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<Bundle<T, D>>+'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize, logging: Option<Logger>) -> (Self::Pusher, Self::Puller);
}

/// A direct connection
pub struct Pipeline;
impl<T: 'static, D: 'static> ParallelizationContract<T, D> for Pipeline {
    type Pusher = LogPusher<T, D, ThreadPusher<Bundle<T, D>>>;
    type Puller = LogPuller<T, D, ThreadPuller<Bundle<T, D>>>;
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize, logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        // ignore `&mut A` and use thread allocator
        let (pusher, puller) = Thread::new::<Bundle<T, D>>();
        (LogPusher::new(pusher, allocator.index(), allocator.index(), identifier, logging.clone()),
         LogPuller::new(puller, allocator.index(), identifier, logging.clone()))
    }
}

/// An exchange between multiple observers by data
pub struct Exchange<D, F: Fn(&D)->u64+'static> { hash_func: F, phantom: PhantomData<D>, }
impl<D, F: Fn(&D)->u64> Exchange<D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> Exchange<D, F> {
        Exchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Eq+Data+Abomonation+Clone, D: Data+Abomonation+Clone, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    // TODO: The closure in the type prevents us from naming it.
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<Push<Bundle<T, D>>>;
    type Puller = Box<Pull<Bundle<T, D>>>;
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize, logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>(identifier);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        (Box::new(ExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Box::new(LogPuller::new(receiver, allocator.index(), identifier, logging.clone())))
    }
}

// /// An exchange between multiple observers by time and data
// pub struct TimeExchange<D, T, F: Fn(&T, &D)->u64+'static> { hash_func: F, phantom: PhantomData<(T, D)>, }
// impl<D, T, F: Fn(&T, &D)->u64> TimeExchange<D, T, F> {
//     /// Allocates a new `TimeExchange` pact from a distribution function.
//     pub fn new(func: F) -> TimeExchange<D, T, F> {
//         TimeExchange {
//             hash_func:  func,
//             phantom:    PhantomData,
//         }
//     }
// }

// impl<T: Eq+Data+Abomonation+Clone, D: Data+Abomonation+Clone, F: Fn(&T, &D)->u64+'static> ParallelizationContract<T, D> for TimeExchange<D, T, F> {
//     type Pusher = ExchangePusher<T, D, Pusher<T, D, Box<Push<CommMessage<Message<T, D>>>>>, F>;
//     type Puller = Puller<T, D, Box<Pull<CommMessage<Message<T, D>>>>>;
//     fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize, logging: Logger) -> (Self::Pusher, Self::Puller) {
//         let (senders, receiver, channel_id) = allocator.allocate::<Message<T, D>>();
//         let senders = senders.into_iter().enumerate().map(|(i,x)| Pusher::new(x, allocator.index(), i, identifier, channel_id, logging.clone())).collect::<Vec<_>>();
//         (ExchangePusher::new(senders, self.hash_func), Puller::new(receiver, allocator.index(), identifier, channel_id, logging.clone()))
//     }
// }


/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
pub struct LogPusher<T, D, P: Push<Bundle<T, D>>> {
    pusher: P,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
    phantom: ::std::marker::PhantomData<(T, D)>,
    logging: Option<Logger>,
}
impl<T, D, P: Push<Bundle<T, D>>> LogPusher<T, D, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, source: usize, target: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPusher {
            pusher,
            channel,
            counter: 0,
            source,
            target,
            phantom: ::std::marker::PhantomData,
            logging,
        }
    }
}

impl<T, D, P: Push<Bundle<T, D>>> Push<Bundle<T, D>> for LogPusher<T, D, P> {
    #[inline(always)]
    fn push(&mut self, pair: &mut Option<Bundle<T, D>>) {
        if let Some(bundle) = pair {
            self.counter += 1;
            self.logging.as_ref().map(|l| l.log(::logging::MessagesEvent {
                is_send: true,
                channel: self.channel,
                source: self.source,
                target: self.target,
                seq_no: self.counter-1,
                length: bundle.data.len(),
            }));
        }
        self.pusher.push(pair);
    }
}

/// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
pub struct LogPuller<T, D, P: Pull<Bundle<T, D>>> {
    puller: P,
    channel: usize,
    index: usize,
    phantom: ::std::marker::PhantomData<(T, D)>,
    logging: Option<Logger>,
}
impl<T, D, P: Pull<Bundle<T, D>>> LogPuller<T, D, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPuller {
            puller,
            channel,
            index,
            phantom: ::std::marker::PhantomData,
            logging,
        }
    }
}

impl<T, D, P: Pull<Bundle<T, D>>> Pull<Bundle<T, D>> for LogPuller<T, D, P> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<Bundle<T,D>> {
        let result = self.puller.pull();
        if let Some(bundle) = result {
            let channel = self.channel;
            let target = self.index;
            self.logging.as_ref().map(|l| l.log(::logging::MessagesEvent {
                is_send: false,
                channel,
                source: bundle.from,
                target,
                seq_no: bundle.seq,
                length: bundle.data.len(),
            }));
        }
        result
    }
}
