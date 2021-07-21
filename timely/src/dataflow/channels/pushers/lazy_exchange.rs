//! The exchange pattern distributes pushed data between many target pushees.

use std::marker::PhantomData;

use crate::{Data, ExchangeData};
use crate::communication::{Pull, Push};
use crate::dataflow::channels::pact::{ParallelizationContract, LogPusher, LogPuller};
use crate::dataflow::channels::{Bundle, Message};
use crate::logging::TimelyLogger as Logger;
use crate::worker::AsWorker;

/// Distributes records among target pushees according to a distribution function.
///
/// This implementation behaves similarly to [crate::dataflow::channels::pushers::Exchange], but
/// tries to leave less allocations around. It does not preallocate a buffer for each pushee, but
/// only allocates it once data is pushed. On flush, the allocation is passed to the pushee, and
/// only what it is passed back is retained.
pub struct LazyExchangePusher<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64> {
    pushers: Vec<P>,
    buffers: Vec<Vec<D>>,
    current: Option<T>,
    hash_func: H,
}

impl<T: Clone, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> LazyExchangePusher<T, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> LazyExchangePusher<T, D, P, H> {
        let buffers = (0..pushers.len()).map(|_| vec![]).collect();
        LazyExchangePusher {
            pushers,
            hash_func: key,
            buffers,
            current: None,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        if !self.buffers[index].is_empty() {
            if let Some(ref time) = self.current {
                Message::push_at_no_allocation(&mut self.buffers[index], time.clone(), &mut self.pushers[index]);
            }
        }
    }
}

impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> Push<Bundle<T, D>> for LazyExchangePusher<T, D, P, H> {
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        } else if let Some(message) = message {
            let message = message.as_mut();
            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            if self.current.as_ref().map_or(false, |x| x != time) {
                for index in 0..self.pushers.len() {
                    self.flush(index);
                }
            }
            self.current = Some(time.clone());

            // if the number of pushers is a power of two, use a mask
            if (self.pushers.len() & (self.pushers.len() - 1)) == 0 {
                let mask = (self.pushers.len() - 1) as u64;
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) & mask) as usize;
                    // Push at the target buffer, which might be without capacity, or preallocated
                    self.buffers[index].push(datum);
                    // We have reached the buffer's capacity
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        // If the buffer's capacity is below the default length, reallocate to match
                        // the default length
                        if self.buffers[index].capacity() < Message::<T, D>::default_length() {
                            let to_reserve = Message::<T, D>::default_length() - self.buffers[index].capacity();
                            self.buffers[index].reserve(to_reserve);
                        } else {
                            // Buffer is at capacity, flush
                            self.flush(index);
                            // Explicitly allocate a new buffer under the assumption that more data
                            // will be sent to the pushee.
                            if self.buffers[index].capacity() < Message::<T, D>::default_length() {
                                let to_reserve =  Message::<T, D>::default_length() - self.buffers[index].capacity();
                                self.buffers.reserve(to_reserve);
                            }
                        }
                    }
                }
            } else {
                // as a last resort, use mod (%)
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) % self.pushers.len() as u64) as usize;
                    self.buffers[index].push(datum);
                    // This code is duplicated from above, keep in sync!
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        if self.buffers[index].capacity() < Message::<T, D>::default_length() {
                            let to_reserve = Message::<T, D>::default_length() - self.buffers[index].capacity();
                            self.buffers[index].reserve(to_reserve);
                        } else {
                            self.flush(index);
                            if self.buffers[index].capacity() < Message::<T, D>::default_length() {
                                let to_reserve =  Message::<T, D>::default_length() - self.buffers[index].capacity();
                                self.buffers.reserve(to_reserve);
                            }
                        }
                    }
                }
            }
        } else {
            // flush
            for index in 0..self.pushers.len() {
                self.flush(index);
                self.pushers[index].push(&mut None);
            }
        }
    }
}

/// An exchange between multiple observers by data, backed by [LazyExchangePusher].
pub struct LazyExchange<D, F> { hash_func: F, phantom: PhantomData<D> }

impl<D, F: FnMut(&D)->u64+'static> LazyExchange<D, F> {
    /// Allocates a new `LeanExchange` pact from a distribution function.
    pub fn new(func: F) -> Self {
        Self {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Eq+ExchangeData, D: ExchangeData, F: FnMut(&D)->u64+'static> ParallelizationContract<T, D> for LazyExchange<D, F> {
    // TODO: The closure in the type prevents us from naming it.
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<dyn Push<Bundle<T, D>>>;
    type Puller = Box<dyn Pull<Bundle<T, D>>>;
    fn connect<A: AsWorker>(mut self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>(identifier, address);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        (Box::new(LazyExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Box::new(LogPuller::new(receiver, allocator.index(), identifier, logging.clone())))
    }
}

impl<D, F> std::fmt::Debug for LazyExchange<D, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyExchange").finish()
    }
}
