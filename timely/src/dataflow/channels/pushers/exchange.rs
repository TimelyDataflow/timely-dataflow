//! The exchange pattern distributes pushed data between many target pushees.

use std::marker::PhantomData;

use crate::Data;
use crate::communication::Push;
use crate::dataflow::channels::{Bundle, Message};

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct ExchangePusherGeneric<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64, B: ExchangeBehavior<T, D>> {
    pushers: Vec<P>,
    buffers: Vec<Vec<D>>,
    current: Option<T>,
    hash_func: H,
    _phantom_data: PhantomData<B>,
}

/// The behavior of an exchange specialization
///
/// This trait gives specialized exchange implementations the opportunity to hook into interesting
/// places for memory management. It exposes the lifecycle of each of the pushee's buffers, starting
/// from creation, ensuring allocations, flushing and finalizing.
pub trait ExchangeBehavior<T, D> {
    /// Allocate a new buffer, called while creating the exchange pusher.
    fn allocate() -> Vec<D>;
    /// Check the buffer's capacity before pushing a single element.
    fn check(buffer: &mut Vec<D>);
    /// Flush a buffer's contents, either when the buffer is at capacity or when no more data is
    /// available.
    fn flush<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P);
    /// Finalize a buffer after pushing `None`, i.e. no more data is available.
    fn finalize(buffer: &mut Vec<D>);
}

/// Exchange behavior that always has push buffers fully allocated.
pub struct FullyAllocatedExchangeBehavior {}

impl<T, D> ExchangeBehavior<T, D> for FullyAllocatedExchangeBehavior {
    fn allocate() -> Vec<D> {
        Vec::with_capacity(Message::<T, D>::default_length())
    }

    fn check(_buffer: &mut Vec<D>) {
        // Not needed, always allocated
    }

    fn flush<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {
        // `push_at` ensures an allocation.
        Message::push_at(buffer, time, pusher);
    }

    fn finalize(_buffer: &mut Vec<D>) {
        // retain any allocation
    }
}

/// Default exchange type is to fully allocate exchange buffers
pub type Exchange<T, D, P, H> = ExchangePusherGeneric<T, D, P, H, FullyAllocatedExchangeBehavior>;

impl<T: Clone, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64, B: ExchangeBehavior<T, D>>  ExchangePusherGeneric<T, D, P, H, B> {
    /// Allocates a new `ExchangeGeneric` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> ExchangePusherGeneric<T, D, P, H, B> {
        let mut buffers = vec![];
        for _ in 0..pushers.len() {
            buffers.push(B::allocate());
        }
        ExchangePusherGeneric {
            pushers,
            hash_func: key,
            buffers,
            current: None,
            _phantom_data: PhantomData,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        if !self.buffers[index].is_empty() {
            if let Some(ref time) = self.current {
                B::flush(&mut self.buffers[index], time.clone(), &mut self.pushers[index]);
            }
        }
    }
}

impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64, B: ExchangeBehavior<T, D>> Push<Bundle<T, D>> for ExchangePusherGeneric<T, D, P, H, B> {
    #[inline(never)]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        }
        else if let Some(message) = message {

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
                    B::check(&mut self.buffers[index]);
                    self.buffers[index].push(datum);
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }

                    // unsafe {
                    //     self.buffers.get_unchecked_mut(index).push(datum);
                    //     if self.buffers.get_unchecked(index).len() == self.buffers.get_unchecked(index).capacity() {
                    //         self.flush(index);
                    //     }
                    // }

                }
            }
            // as a last resort, use mod (%)
            else {
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) % self.pushers.len() as u64) as usize;
                    B::check(&mut self.buffers[index]);
                    self.buffers[index].push(datum);
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }
                }
            }

        }
        else {
            // flush
            for index in 0..self.pushers.len() {
                self.flush(index);
                self.pushers[index].push(&mut None);
                B::finalize(&mut self.buffers[index]);
            }
        }
    }
}
