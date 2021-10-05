//! The exchange pattern distributes pushed data between many target pushees.

use crate::Data;
use crate::communication::{Push, Container};
use crate::dataflow::channels::{Bundle, Message};
use crate::progress::Timestamp;

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64> {
    pushers: Vec<P>,
    buffers: Vec<Vec<D>>,
    current: Option<T>,
    hash_func: H,
}

impl<T: Timestamp+Clone, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64>  Exchange<T, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, D, P, H> {
        let mut buffers = vec![];
        for _ in 0..pushers.len() {
            buffers.push(Vec::with_capacity(crate::communication::container::buffer::default_capacity::<D>()));
        }
        Exchange {
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
                let mut allocation = None;
                Message::push_at(Some(::std::mem::take(&mut self.buffers[index])), time.clone(), &mut self.pushers[index], &mut allocation);
                if let Some(allocation) = allocation {
                    self.buffers[index] = allocation;
                }
                self.buffers[index].ensure_capacity();
            }
        }
    }
}

impl<T: Timestamp, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> Push<Bundle<T, D>> for Exchange<T, D, P, H> {
    #[inline(never)]
    fn push(&mut self, message: Option<Bundle<T, D>>, allocation: &mut Option<<Bundle<T, D> as Container>::Allocation>) {
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
