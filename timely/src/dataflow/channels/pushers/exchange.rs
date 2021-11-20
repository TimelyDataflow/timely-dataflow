//! The exchange pattern distributes pushed data between many target pushees.

use crate::Data;
use crate::communication::Push;
use crate::dataflow::channels::{Bundle, Message};

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64> {
    pushers: Vec<P>,
    buffers: Vec<Vec<D>>,
    current: Option<T>,
    hash_func: H,
}

impl<T: Clone, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64>  Exchange<T, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, D, P, H> {
        let mut buffers = vec![];
        for _ in 0..pushers.len() {
            buffers.push(Vec::new());
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
                Message::push_at_no_allocation(&mut self.buffers[index], time.clone(), &mut self.pushers[index]);
            }
        }
    }
}

impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> Push<Bundle<T, D>> for Exchange<T, D, P, H> {
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

                    // Ensure allocated buffers: If the buffer's capacity is less than its default
                    // capacity, increase the capacity such that it matches the default.
                    if self.buffers[index].capacity() < Message::<T, D>::default_length() {
                        let to_reserve = Message::<T, D>::default_length() - self.buffers[index].capacity();
                        self.buffers[index].reserve(to_reserve);
                    }
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
                    // Ensure allocated buffers: If the buffer's capacity is less than its default
                    // capacity, increase the capacity such that it matches the default.
                    if self.buffers[index].capacity() < Message::<T, D>::default_length() {
                        let to_reserve = Message::<T, D>::default_length() - self.buffers[index].capacity();
                        self.buffers[index].reserve(to_reserve);
                    }
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
            }
        }
    }
}
