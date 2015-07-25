use communication::{Message, Observer};
use serialization::Serializable;
use drain::DrainExt;

// an observer routing between multiple observers
// TODO : Software write combining
pub struct Exchange<O: Observer, H: Fn(&O::Data) -> u64> {
    observers:  Vec<O>,
    hash_func:  H,
    buffers:    Vec<Vec<O::Data>>,
}

impl<O: Observer, H: Fn(&O::Data) -> u64> Exchange<O, H> {
    pub fn new(obs: Vec<O>, key: H) -> Exchange<O, H> {
        let mut buffers = vec![]; for _ in 0..obs.len() { buffers.push(Vec::with_capacity(4096)); }
        Exchange {
            observers: obs,
            hash_func: key,
            buffers: buffers,
        }
    }
    fn flush(&mut self, index: usize) {
        let mut message = Message::Typed(::std::mem::replace(&mut self.buffers[index], Vec::new()));
        self.observers[index].give(&mut message);
        self.buffers[index] = if let Message::Typed(mut data) = message { data.clear(); data }
                              else { Vec::with_capacity(4096) };
    }
}
impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for Exchange<O, H> where O::Data: Clone+Serializable {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) -> () {
        for observer in self.observers.iter_mut() { observer.open(time); }
    }
    #[inline] fn shut(&mut self, time: &O::Time) -> () {
        for index in 0..self.buffers.len() {
            if self.buffers[index].len() > 0 {
                self.flush(index);
            }
        }

        for observer in self.observers.iter_mut() { observer.shut(time); }
    }
    #[inline] fn give(&mut self, data: &mut Message<O::Data>) {
        // cheeky optimization for single thread
        if self.observers.len() == 1 {
            self.observers[0].give(data);
        }
        // if the number of observers is a power of two, use a mask
        else if (self.observers.len() & (self.observers.len() - 1)) == 0 {
            let mask = (self.observers.len() - 1) as u64;
            for datum in data.take().drain_temp() {
                let index = (((self.hash_func)(&datum)) & mask) as usize;
                self.buffers[index].push(datum);
                if self.buffers[index].len() == self.buffers[index].capacity() {
                    self.flush(index);
                }
            }
        }
        // as a last resort, use mod (%)
        else {
            for datum in data.take().drain_temp() {
                let index = (((self.hash_func)(&datum)) % self.observers.len() as u64) as usize;
                self.buffers[index].push(datum);
                if self.buffers[index].len() == self.buffers[index].capacity() {
                    self.flush(index);
                }
            }
        }
    }
}
