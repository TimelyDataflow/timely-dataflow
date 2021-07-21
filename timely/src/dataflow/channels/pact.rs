//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: AsWorker`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::{fmt::{self, Debug}, marker::PhantomData};

use crate::communication::{Push, Pull, Data};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};

use crate::worker::AsWorker;
use crate::dataflow::channels::pushers::Exchange as ExchangePusher;
use super::{Bundle, Message};

use crate::logging::{TimelyLogger as Logger, MessagesEvent};
use std::cell::RefCell;
use std::rc::Rc;

/// A `ParallelizationContract` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContract<T: 'static, D: 'static> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<Bundle<T, D>>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<Bundle<T, D>>+'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Rc<RefCell<Logger>>>) -> (Self::Pusher, Self::Puller);
}

/// A direct connection
#[derive(Debug)]
pub struct Pipeline;

impl<T: 'static, D: 'static> ParallelizationContract<T, D> for Pipeline {
    type Pusher = LogPusher<T, D, ThreadPusher<Bundle<T, D>>>;
    type Puller = LogPuller<T, D, ThreadPuller<Bundle<T, D>>>;
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Rc<RefCell<Logger>>>) -> (Self::Pusher, Self::Puller) {
        let (pusher, puller) = allocator.pipeline::<Message<T, D>>(identifier, address);
        // // ignore `&mut A` and use thread allocator
        // let (pusher, puller) = Thread::new::<Bundle<T, D>>();
        (LogPusher::new(pusher, allocator.index(), allocator.index(), identifier, logging.clone()),
         LogPuller::new(puller, allocator.index(), identifier, logging))
    }
}

/// An exchange between multiple observers by data
pub struct Exchange<D, F> { hash_func: F, phantom: PhantomData<D> }

impl<D, F: FnMut(&D)->u64+'static> Exchange<D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> Exchange<D, F> {
        Exchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Eq+Data+Clone, D: Data+Clone, F: FnMut(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    // TODO: The closure in the type prevents us from naming it.
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<dyn Push<Bundle<T, D>>>;
    type Puller = Box<dyn Pull<Bundle<T, D>>>;
    fn connect<A: AsWorker>(mut self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Rc<RefCell<Logger>>>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>(identifier, address);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        (Box::new(ExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Box::new(LogPuller::new(receiver, allocator.index(), identifier, logging.clone())))
    }
}

impl<D, F> Debug for Exchange<D, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchange").finish()
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPusher<T, D, P: Push<Bundle<T, D>>> {
    pusher: P,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
    phantom: PhantomData<(T, D)>,
    logging: Option<Rc<RefCell<Logger>>>,
}

impl<T, D, P: Push<Bundle<T, D>>> LogPusher<T, D, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, source: usize, target: usize, channel: usize, logging: Option<Rc<RefCell<Logger>>>) -> Self {
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

impl<T, D, P: Push<Bundle<T, D>>> Push<Bundle<T, D>> for LogPusher<T, D, P> {
    #[inline]
    fn push(&mut self, pair: &mut Option<Bundle<T, D>>) {
        if let Some(bundle) = pair {
            self.counter += 1;

            // Stamp the sequence number and source.
            // FIXME: Awkward moment/logic.
            if let Some(message) = bundle.if_mut() {
                message.seq = self.counter - 1;
                message.from = self.source;
            }

            if let Some(logger) = self.logging.as_ref() {
                logger.borrow().log(MessagesEvent {
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
pub struct LogPuller<T, D, P: Pull<Bundle<T, D>>> {
    puller: P,
    channel: usize,
    index: usize,
    phantom: PhantomData<(T, D)>,
    logging: Option<Rc<RefCell<Logger>>>,
}

impl<T, D, P: Pull<Bundle<T, D>>> LogPuller<T, D, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize, logging: Option<Rc<RefCell<Logger>>>) -> Self {
        LogPuller {
            puller,
            channel,
            index,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, D, P: Pull<Bundle<T, D>>> Pull<Bundle<T, D>> for LogPuller<T, D, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<Bundle<T,D>> {
        let result = self.puller.pull();
        if let Some(bundle) = result {
            let channel = self.channel;
            let target = self.index;

            if let Some(logger) = self.logging.as_ref() {
                logger.borrow().log(MessagesEvent {
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
