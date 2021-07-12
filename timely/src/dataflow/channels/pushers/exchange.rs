//! The exchange pattern distributes pushed data between many target pushees.

use crate::communication::Push;
use crate::Data;
use crate::dataflow::channels::{Bundle, Message};

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64> {
    pushers: Vec<P>,
    buffers: Vec<Vec<D>>,
    max_len: Vec<usize>,
    current: Option<T>,
    hash_func: H,
}

impl<T: Clone, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64>  Exchange<T, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, D, P, H> {
        let mut buffers = vec![];
        for _ in 0..pushers.len() {
            buffers.push(Vec::with_capacity(Message::<T, D>::minimal_length()));
        }
        Exchange {
            pushers,
            hash_func: key,
            max_len: vec![0; buffers.len()],
            buffers,
            current: None,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize, maybe_shrink: bool) {
        if !self.buffers[index].is_empty() {
            let old_len = self.buffers[index].len();
            if let Some(ref time) = self.current {
                Message::push_at(&mut self.buffers[index], time.clone(), &mut self.pushers[index]);
            }
            if maybe_shrink {
                if self.buffers[index].capacity() > 2 * self.max_len[index] &&
                    self.buffers[index].capacity() > Message::<T, D>::minimal_length() {
                    self.buffers[index] = Vec::with_capacity(Message::<T, D>::minimal_length());
                }
                self.max_len[index] = 0;
            } else {
                self.max_len[index] = std::cmp::max(self.max_len[index], old_len);
            }
        }
    }
}

impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> Push<Bundle<T, D>> for Exchange<T, D, P, H> {
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        let push_at = |this: &mut Exchange<_, _, _, _>, index: usize, datum| {
            this.buffers[index].push(datum);
            if this.buffers[index].len() == this.buffers[index].capacity() {
                this.flush(index, false);
                if this.buffers[index].len() < Message::<T, D>::default_length() {
                    let len = this.buffers[index].len();
                    this.buffers[index].reserve(len);
                }
            }
        };
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
                    self.flush(index, false);
                }
            }
            self.current = Some(time.clone());

            // if the number of pushers is a power of two, use a mask
            if (self.pushers.len() & (self.pushers.len() - 1)) == 0 {
                let mask = (self.pushers.len() - 1) as u64;
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) & mask) as usize;
                    push_at(self, index, datum);
                }
            } else {
                // as a last resort, use mod (%)
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) % self.pushers.len() as u64) as usize;
                    push_at(self, index, datum);
                }
            }
        } else {
            // flush
            for index in 0..self.pushers.len() {
                self.flush(index, true);
                self.pushers[index].push(&mut None);
            }
        }
    }
}
