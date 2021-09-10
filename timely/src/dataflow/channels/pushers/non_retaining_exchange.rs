//! The exchange pattern distributes pushed data between many target pushees.

use std::marker::PhantomData;

use crate::ExchangeData;
use crate::communication::{Pull, Push};
use crate::dataflow::channels::pact::{ParallelizationContract, LogPusher, LogPuller};
use crate::dataflow::channels::pushers::exchange::{ExchangeBehavior, ExchangePusherGeneric};
use crate::dataflow::channels::{Bundle, Message};
use crate::logging::TimelyLogger as Logger;
use crate::worker::AsWorker;

/// Distributes records among target pushees according to a distribution function.
///
/// This implementation behaves similarly to [crate::dataflow::channels::pushers::Exchange], but
/// leaves no allocations around. It does not preallocate a buffer for each pushee, but
/// only allocates it once data is pushed. On flush, the allocation is passed to the pushee, and
/// any returned allocation will be dropped.
pub struct NonRetainingExchangeBehavior {}

impl<T, D> ExchangeBehavior<T, D> for NonRetainingExchangeBehavior {
    fn allocate() -> Vec<D> {
        Vec::new()
    }

    fn check(buffer: &mut Vec<D>) {
        if buffer.capacity() < Message::<T, D>::default_length() {
            let to_reserve = Message::<T, D>::default_length() - buffer.capacity();
            buffer.reserve(to_reserve);
        }
    }

    fn flush<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {
        Message::push_at_no_allocation(buffer, time, pusher);
    }

    fn finalize(buffer: &mut Vec<D>) {
        *buffer = Vec::new();
    }
}

/// Non-retaining exchange pusher definition
pub type NonRetainingExchangePusher<T, D, P, H> = ExchangePusherGeneric<T, D, P, H, NonRetainingExchangeBehavior>;

/// An exchange between multiple observers by data, backed by [NonRetainingExchangePusher].
pub struct NonRetainingExchange<D, F> { hash_func: F, phantom: PhantomData<D> }

impl<D, F: FnMut(&D)->u64+'static> NonRetainingExchange<D, F> {
    /// Allocates a new exchange pact from a distribution function.
    pub fn new(func: F) -> Self {
        Self {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Eq+ExchangeData, D: ExchangeData, F: FnMut(&D)->u64+'static> ParallelizationContract<T, D> for NonRetainingExchange<D, F> {
    // TODO: The closure in the type prevents us from naming it.
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<dyn Push<Bundle<T, D>>>;
    type Puller = Box<dyn Pull<Bundle<T, D>>>;
    fn connect<A: AsWorker>(mut self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>(identifier, address);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        (Box::new(NonRetainingExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Box::new(LogPuller::new(receiver, allocator.index(), identifier, logging.clone())))
    }
}

impl<D, F> std::fmt::Debug for NonRetainingExchange<D, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonRetainingExchange").finish()
    }
}
