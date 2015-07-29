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
// TODO : wouldn't clear out memory forcibly, just wouldn't insist on re-alloc of stolen buffers.
impl<O: Observer, H: Fn(&O::Data) -> u64> Exchange<O, H> {
    pub fn new(obs: Vec<O>, key: H) -> Exchange<O, H> {
        let mut buffers = vec![]; for _ in 0..obs.len() { buffers.push(Vec::new()); }
        Exchange {
            observers: obs,
            hash_func: key,
            buffers: buffers,
        }
    }
    fn flush(&mut self, index: usize, ensure_allocation: Option<usize>) {
        let mut message = Message::from_typed(&mut self.buffers[index]);
        self.observers[index].give(&mut message);
        self.buffers[index] = message.into_typed();
        if let Some(capacity) = ensure_allocation {
            if self.buffers[index].capacity() != capacity {
                // ALLOC : would expect to overwrite a Vec::new(), but ... who knows...
                assert!(self.buffers[index].capacity() == 0);
                self.buffers[index] = Vec::with_capacity(capacity);
            }
        }
    }
}
impl<O: Observer, H: Fn(&O::Data) -> u64> Observer for Exchange<O, H> where O::Data: Clone+Serializable {
    type Time = O::Time;
    type Data = O::Data;
    #[inline] fn open(&mut self, time: &O::Time) -> () {
        for (index, observer) in self.observers.iter_mut().enumerate() {
            observer.open(time);
            if self.buffers[index].capacity() != Message::<O::Data>::default_length() {
                assert!(self.buffers[index].capacity() == 0);
                self.buffers[index] = Vec::with_capacity(Message::<O::Data>::default_length());
            }
        }
    }
    #[inline] fn shut(&mut self, time: &O::Time) -> () {
        for index in 0..self.buffers.len() {
            if self.buffers[index].len() > 0 {
                self.flush(index, None);
            }
        }

        for observer in self.observers.iter_mut() { observer.shut(time); }
    }
    // ALLOC : This is a point where might cause an allocation for serialized data that needs to
    // ALLOC : be exchanged. In a perfect world, we would .iter().cloned() serialized data,
    // ALLOC : avoiding the Vec allocation.
    #[inline] fn give(&mut self, data: &mut Message<O::Data>) {
        // cheeky optimization for single thread
        if self.observers.len() == 1 {
            self.observers[0].give(data);
        }
        // if the number of observers is a power of two, use a mask
        else if (self.observers.len() & (self.observers.len() - 1)) == 0 {
            let mask = (self.observers.len() - 1) as u64;
            for datum in data.drain_temp() {
                let index = (((self.hash_func)(&datum)) & mask) as usize;
                // assert!(self.buffers[index].capacity() == Message::<O::Data>::default_length());
                self.buffers[index].push(datum);
                if self.buffers[index].len() == self.buffers[index].capacity() {
                    self.flush(index, Some(Message::<O::Data>::default_length()));
                }
            }
        }
        // as a last resort, use mod (%)
        else {
            for datum in data.drain_temp() {
                let index = (((self.hash_func)(&datum)) % self.observers.len() as u64) as usize;
                // assert!(self.buffers[index].capacity() == Message::<O::Data>::default_length());
                self.buffers[index].push(datum);
                if self.buffers[index].len() == self.buffers[index].capacity() {
                    self.flush(index, Some(Message::<O::Data>::default_length()));
                }
            }
        }
    }
}
