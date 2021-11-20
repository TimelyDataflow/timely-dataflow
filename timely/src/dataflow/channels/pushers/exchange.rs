//! The exchange pattern distributes pushed data between many target pushees.

use std::mem::MaybeUninit;
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
    sort_buffer: Vec<(usize, usize)>,
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
            sort_buffer: Vec::new(),
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

    /// Partition data according to an index function
    #[inline(always)]
    fn partition<F: Fn(u64) -> usize>(&mut self, time: &T, data: &mut Vec<D>, func: F) {
        // move sort_buffer to avoid borrowing `mut self` twice
        let mut sort_buffer = ::std::mem::take(&mut self.sort_buffer);
        // Ensure that the sort buffer has as many entries as the incoming data
        if sort_buffer.capacity() < data.len() {
            let to_reserve = data.len() - sort_buffer.capacity();
            sort_buffer.reserve(to_reserve);
        }

        // Calculate the (target, data_index) per datum
        for (datum_index, datum) in data.iter().enumerate() {
            let index = (func)((self.hash_func)(time, datum));
            sort_buffer.push((index, datum_index));
        }

        // Sort the sort buffer by index
        sort_buffer.sort();

        // Convert the provided data such that we can move out of it and leave gaps behind
        // This is safe because every element will be accessed exactly once.
        let local_data = ::std::mem::take(data);
        let mut local_data: Vec<MaybeUninit<D>> = unsafe { ::std::mem::transmute(local_data) };

        for (index, offset) in sort_buffer.drain(..) {
            // Read the element from the data vector
            let datum = unsafe { ::std::ptr::read(local_data[offset].as_ptr()) };

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

        // Clear the local data, which is safe because it will not execute drop.
        local_data.clear();
        // Restore the data allocation by transmuting the cleared data back to its original allocation
        *data = unsafe { std::mem::transmute(local_data) };
        self.sort_buffer = sort_buffer;
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
                self.partition(time, data, move |hash| (hash & mask) as usize);
            }
            // as a last resort, use mod (%)
            else {
                let pushers = self.pushers.len() as u64;
                self.partition(time, data, move |hash| (hash % pushers) as usize);
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
