//! Broadcast records to all workers.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::communication::{Pull, Push};
use crate::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContract};
use crate::dataflow::channels::{Bundle, Message};
use crate::dataflow::operators::Operator;
use crate::dataflow::{Scope, Stream};
use crate::logging::TimelyLogger;
use crate::worker::AsWorker;
use crate::ExchangeData;

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
    /// Broadcast records to all workers.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Broadcast, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .broadcast()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn broadcast(&self) -> Self;
}

impl<G: Scope, D: ExchangeData> Broadcast<D> for Stream<G, D> {
    fn broadcast(&self) -> Self {
        let mut vector = Vec::new();
        self.unary(BroadcastExchange::new(), "Broadcast", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    output.session(&time).give_vec(&mut vector);
                });
            }
        })
    }
}

/// An exchange between multiple observers by data
pub struct BroadcastExchange<D> {
    phantom: PhantomData<D>,
}

impl<'a, D: ExchangeData> BroadcastExchange<D> {
    /// Allocates a new `BroadcastExchange` pact
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<'a, T: Eq + ExchangeData, D: ExchangeData> ParallelizationContract<T, D>
    for BroadcastExchange<D>
{
    type Pusher = BroadcastPusher<T, D, LogPusher<T, D, Box<dyn Push<Bundle<T, D>>>>>;
    type Puller = LogPuller<T, D, Box<dyn Pull<Bundle<T, D>>>>;
    fn connect<A: AsWorker>(
        self,
        allocator: &mut A,
        identifier: usize,
        address: &[usize],
        logging: Option<TimelyLogger>,
    ) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>(identifier, address);
        let senders = senders
            .into_iter()
            .enumerate()
            .map(|(i, x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone()))
            .collect::<Vec<_>>();
        (
            BroadcastPusher::new(senders),
            LogPuller::new(receiver, allocator.index(), identifier, logging.clone()),
        )
    }
}

impl<C> std::fmt::Debug for BroadcastExchange<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastExchange").finish()
    }
}

/// A pusher that broadcasts its messages to all senders
pub struct BroadcastPusher<T, D: ExchangeData, P: Push<Bundle<T, D>>> {
    pushers: Vec<P>,
    _phantom_data: PhantomData<(T, D)>,
}

impl<T: Clone, D: ExchangeData, P: Push<Bundle<T, D>>> BroadcastPusher<T, D, P> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>) -> Self {
        Self {
            pushers,
            _phantom_data: PhantomData,
        }
    }
}

impl<T: ExchangeData, D: ExchangeData, P: Push<Bundle<T, D>>> Push<Bundle<T, D>>
    for BroadcastPusher<T, D, P>
{
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        } else {
            if let Some(bundle) = message {
                let arc = Arc::new(bundle.as_mut().clone());
                for pusher in &mut self.pushers {
                    let message = crate::communication::Message::from_arc(Arc::clone(&arc));
                    pusher.push(&mut Some(message))
                }
            } else {
                for pusher in &mut self.pushers {
                    pusher.push(&mut None)
                }
            }
        }
    }
}
