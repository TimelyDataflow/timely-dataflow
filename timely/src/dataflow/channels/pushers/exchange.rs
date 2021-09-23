//! The exchange pattern distributes pushed data between many target pushees.

use std::marker::PhantomData;

use crate::{DataflowContainer, ContainerBuilder, Data, DrainContainer};
use crate::communication::Push;
use crate::dataflow::channels::{BundleCore, Message, MessageAllocation};

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, C: DataflowContainer, D, P: Push<BundleCore<T, C>>, H: FnMut(&T, &D) -> u64> {
    pushers: Vec<P>,
    buffers: Vec<C::Builder>,
    current: Option<T>,
    hash_func: H,
    _phantom_data: PhantomData<D>,
}

impl<T: Clone, C: DataflowContainer, D, P: Push<BundleCore<T, C>>, H: FnMut(&T, &D)->u64>  Exchange<T, C, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, C, D, P, H> {
        let mut buffers = vec![];
        for _ in 0..pushers.len() {
            buffers.push(C::Builder::with_capacity(C::default_length()));
        }
        Exchange {
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
                let mut container = C::Builder::take_ref(&mut self.buffers[index]).build();
                // TODO: allocation
                Message::push_at(Some(container), time.clone(), &mut self.pushers[index], &mut None);
                self.buffers[index] = C::Builder::new();
            }
        }
    }
}

impl<T: Eq+Data, C: crate::communication::Container+ DataflowContainer<Inner=D>, D: Data, P: Push<BundleCore<T, C>>, H: FnMut(&T, &D)->u64> Push<BundleCore<T, C>> for Exchange<T, C, D, P, H>
    where for<'b> &'b mut C: DrainContainer<Inner=D>,
{
    #[inline(never)]
    fn push(&mut self, message: Option<BundleCore<T, C>>, allocation: &mut Option<<BundleCore<T, C> as crate::communication::Container>::Allocation>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message, allocation);
        }
        else if let Some(mut message) = message {

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
                self.pushers[index].push(None, &mut None);
            }
        }
    }
}
