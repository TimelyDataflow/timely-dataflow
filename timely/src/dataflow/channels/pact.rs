//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: AsWorker`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::fmt::Debug;
use std::rc::Rc;

use crate::Accountable;
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::communication::{Push, Pull};
use crate::dataflow::channels::Message;
use crate::logging::TimelyLogger as Logger;
use crate::worker::AsWorker;

/// A `ParallelizationContract` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContract<T, C> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<Message<T, C>>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<Message<T, C>>+'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: Rc<[usize]>, logging: Option<Logger>) -> (Self::Pusher, Self::Puller);
}

/// A direct connection
#[derive(Debug)]
pub struct Pipeline;

impl<T: 'static, C: Accountable + 'static> ParallelizationContract<T, C> for Pipeline {
    type Pusher = LogPusher<ThreadPusher<Message<T, C>>>;
    type Puller = LogPuller<ThreadPuller<Message<T, C>>>;
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: Rc<[usize]>, logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (pusher, puller) = allocator.pipeline::<Message<T, C>>(identifier, address);
        (LogPusher::new(pusher, allocator.index(), allocator.index(), identifier, logging.clone()),
         LogPuller::new(puller, allocator.index(), identifier, logging))
    }
}

pub use exchange::{ExchangeCore, Exchange};
mod exchange {

    use std::{fmt::{self, Debug}, marker::PhantomData};
    use std::rc::Rc;

    use crate::container::{ContainerBuilder, DrainContainer, LengthPreservingContainerBuilder, SizableContainer, CapacityContainerBuilder, PushInto};
    use crate::communication::{Push, Pull};
    use crate::dataflow::channels::pushers::Exchange as ExchangePusher;
    use crate::dataflow::channels::pushers::exchange::DrainContainerDistributor;
    use crate::dataflow::channels::Message;
    use crate::logging::TimelyLogger as Logger;
    use crate::progress::Timestamp;
    use crate::worker::AsWorker;

    use super::{ParallelizationContract, LogPusher, LogPuller};

    /// An exchange between multiple observers by data
    pub struct ExchangeCore<CB, F> { hash_func: F, phantom: PhantomData<CB> }

    /// [ExchangeCore] specialized to vector-based containers.
    pub type Exchange<D, F> = ExchangeCore<CapacityContainerBuilder<Vec<D>>, F>;

    impl<CB, F> ExchangeCore<CB, F>
    where
        CB: LengthPreservingContainerBuilder,
        CB::Container: DrainContainer,
        for<'a> F: FnMut(&<CB::Container as DrainContainer>::Item<'a>)->u64
    {
        /// Allocates a new `Exchange` pact from a distribution function.
        pub fn new_core(func: F) -> ExchangeCore<CB, F> {
            ExchangeCore {
                hash_func:  func,
                phantom:    PhantomData,
            }
        }
    }

    impl<C, F> ExchangeCore<CapacityContainerBuilder<C>, F>
    where
        C: SizableContainer + DrainContainer,
        for<'a> F: FnMut(&C::Item<'a>)->u64
    {
        /// Allocates a new `Exchange` pact from a distribution function.
        pub fn new(func: F) -> ExchangeCore<CapacityContainerBuilder<C>, F> {
            ExchangeCore {
                hash_func:  func,
                phantom:    PhantomData,
            }
        }
    }

    // Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
    impl<T: Timestamp, CB, H> ParallelizationContract<T, CB::Container> for ExchangeCore<CB, H>
    where
        CB: ContainerBuilder<Container: DrainContainer> + for<'a> PushInto<<CB::Container as DrainContainer>::Item<'a>>,
        CB::Container: Send + crate::dataflow::channels::ContainerBytes,
        for<'a> H: FnMut(&<CB::Container as DrainContainer>::Item<'a>) -> u64 + 'static,
    {
        type Pusher = ExchangePusher<
            T,
            LogPusher<Box<dyn Push<Message<T, CB::Container>>>>,
            DrainContainerDistributor<CB, H>
        >;
        type Puller = LogPuller<Box<dyn Pull<Message<T, CB::Container>>>>;

        fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: Rc<[usize]>, logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
            let (senders, receiver) = allocator.allocate::<Message<T, CB::Container>>(identifier, address);
            let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
            let distributor = DrainContainerDistributor::new(self.hash_func, allocator.peers());
            (ExchangePusher::new(senders, distributor), LogPuller::new(receiver, allocator.index(), identifier, logging.clone()))
        }
    }

    impl<C, F> Debug for ExchangeCore<C, F> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Exchange").finish()
        }
    }
}

pub use distributor::DistributorPact;
/// Parallelization contract based on a `Distributor` implementation.
mod distributor {

    use std::rc::Rc;

    use crate::Accountable;
    use crate::communication::{Push, Pull};
    use crate::dataflow::channels::pushers::{Exchange, exchange::Distributor};
    use crate::dataflow::channels::{ContainerBytes, Message};
    use crate::logging::TimelyLogger;
    use crate::progress::Timestamp;
    use crate::worker::AsWorker;

    use super::{ParallelizationContract, LogPusher, LogPuller};

    /// Intended to wrap a function from a `usize` to an `impl Distributor`.
    ///
    /// For a `D: Distributor<C>` and an appropriate builder `D::new(peers: usize)`,
    /// a `DistributorPact(|peers| D::new(peers))` acts as a pact that will use `D`
    /// and distribute containers of type `C`.
    pub struct DistributorPact<B>(pub B);

    impl<T, B, C, D> ParallelizationContract<T, C> for DistributorPact<B>
    where
        T: Timestamp,
        B: FnOnce(usize) -> D,
        C: Accountable + ContainerBytes + Send + 'static,
        D: Distributor<C> + 'static,
    {
        type Pusher = Exchange<
            T,
            LogPusher<Box<dyn Push<Message<T, C>>>>,
            D
        >;
        type Puller = LogPuller<Box<dyn Pull<Message<T, C>>>>;
        fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: Rc<[usize]>, logging: Option<TimelyLogger>) -> (Self::Pusher, Self::Puller) {
            let (senders, receiver) = allocator.allocate::<Message<T, C>>(identifier, address);
            let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
            let distributor = (self.0)(allocator.peers());
            (Exchange::new(senders, distributor), LogPuller::new(receiver, allocator.index(), identifier, logging.clone()))
        }
    }
}

pub use push_pull::{LogPusher, LogPuller};
mod push_pull {

    use crate::Accountable;
    use crate::communication::{Push, Pull};
    use crate::dataflow::channels::Message;
    use crate::logging::{TimelyLogger as Logger, MessagesEvent};

    /// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
    #[derive(Debug)]
    pub struct LogPusher<P> {
        pusher: P,
        channel: usize,
        counter: usize,
        source: usize,
        target: usize,
        logging: Option<Logger>,
    }

    impl<P> LogPusher<P> {
        /// Allocates a new pusher.
        pub fn new(pusher: P, source: usize, target: usize, channel: usize, logging: Option<Logger>) -> Self {
            LogPusher {
                pusher,
                channel,
                counter: 0,
                source,
                target,
                logging,
            }
        }
    }

    impl<T, C: Accountable, P: Push<Message<T, C>>> Push<Message<T, C>> for LogPusher<P> {
        #[inline]
        fn push(&mut self, pair: &mut Option<Message<T, C>>) {
            if let Some(bundle) = pair {
                self.counter += 1;

                // Stamp the sequence number and source.
                // FIXME: Awkward moment/logic.
                bundle.seq = self.counter - 1;
                bundle.from = self.source;

                if let Some(logger) = self.logging.as_ref() {
                    logger.log(MessagesEvent {
                        is_send: true,
                        channel: self.channel,
                        source: self.source,
                        target: self.target,
                        seq_no: self.counter - 1,
                        record_count: bundle.data.record_count(),
                    })
                }
            }

            self.pusher.push(pair);
        }
    }

    /// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
    #[derive(Debug)]
    pub struct LogPuller<P> {
        puller: P,
        channel: usize,
        index: usize,
        logging: Option<Logger>,
    }

    impl<P> LogPuller<P> {
        /// Allocates a new `Puller`.
        pub fn new(puller: P, index: usize, channel: usize, logging: Option<Logger>) -> Self {
            LogPuller {
                puller,
                channel,
                index,
                logging,
            }
        }
    }

    impl<T, C: Accountable, P: Pull<Message<T, C>>> Pull<Message<T, C>> for LogPuller<P> {
        #[inline]
        fn pull(&mut self) -> &mut Option<Message<T, C>> {
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
                        record_count: bundle.data.record_count(),
                    });
                }
            }

            result
        }
    }
}