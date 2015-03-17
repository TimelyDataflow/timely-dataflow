use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::mem::swap;
use core::marker::PhantomData;

use progress::Timestamp;
use progress::count_map::CountMap;
use communication::Data;
use communication::{Communicator, Pullable, Pushable, PushableObserver};
use communication::observer::{ExchangeObserver};

use columnar::Columnar;

pub fn exchange_with<T: Timestamp,
                     D: Data+Columnar,
                     C: Communicator,
                     F: Fn(&D)->u64>(allocator: &mut C,
                                     hash_func: F) -> (ExchangeObserver<PushableObserver<T,D,Box<Pushable<(T,Vec<D>)>>>, F>,
                                                       ExchangeReceiver<T, D>) {
    let (senders, receiver) = allocator.new_channel();

    let exchange_sender = ExchangeObserver {
        observers:  senders.into_iter().map(|x| PushableObserver { data: Vec::new(), pushable: x, phantom: PhantomData }).collect(),
        hash_func:  hash_func,
    };

    let exchange_receiver = ExchangeReceiver {
        receiver:   receiver,
        buffers:    HashMap::new(),
        doubles:    HashMap::new(),
        consumed:   CountMap::new(),
        frontier:   CountMap::new(),
    };

    return (exchange_sender, exchange_receiver);
}

pub struct ExchangeReceiver<T:Timestamp, D:Data> {
    receiver:   Box<Pullable<(T, Vec<D>)>>, // receiver pair for the exchange channel
    buffers:    HashMap<T, Vec<D>>,         // buffers incoming records indexed by time
    doubles:    HashMap<T, Vec<D>>,         // double-buffered to prevent unbounded reading
    consumed:   CountMap<T>,                // retains cumulative messages consumed
    frontier:   CountMap<T>,                // retains un-claimed messages updates
}

impl<T:Timestamp, D:Data> Pullable<(T, Vec<D>)> for ExchangeReceiver<T, D> {
    fn pull(&mut self) -> Option<(T, Vec<D>)> {
        let next_key = self.doubles.keys().next().map(|x| x.clone());
        if let Some(key) = next_key {
            let val = self.doubles.remove(&key).unwrap();
            self.consumed.update(&key, (val.len() as i64));
            return Some((key, val));
        }
        else {
            self.drain();
            swap(&mut self.buffers, &mut self.doubles);
            return None;
        }
    }
}

impl<T:Timestamp, D:Data> ExchangeReceiver<T, D> {
    fn drain(&mut self) {
        while let Some((time, mut data)) = self.receiver.pull() {
            match self.buffers.entry(time)  {
                Occupied(entry) => { entry.into_mut().append(&mut data); },
                Vacant(entry)   => { entry.insert(data); },
            }
        }
    }

    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>) {
        // println!("consumed progress: {:?}", self.consumed);
        while let Some((ref time, value)) = self.consumed.pop() { consumed.update(time, value); }
    }
}
