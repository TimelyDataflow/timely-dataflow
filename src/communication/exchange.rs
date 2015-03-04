use std::collections::HashMap;
use std::mem::swap;
use core::marker::PhantomData;

use progress::Timestamp;
use progress::count_map::CountMap;
use communication::Data;
use communication::{Communicator, Pullable, Pushable, PushableObserver};
use communication::observer::{ExchangeObserver};

use columnar::Columnar;

pub fn exchange_with<T: Timestamp, D: Data, F: Fn(&D)->u64>(allocator: &mut Communicator,
                                                            hash_func: F) -> (ExchangeObserver<PushableObserver<T,D,Box<Pushable<(T,Vec<D>)>>>, F>,
                                                                              ExchangeReceiver<T, D>)
where D : Columnar
{
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

impl<T:Timestamp, D:Data> Iterator for ExchangeReceiver<T, D> {
    type Item = (T, Vec<D>);

    fn next(&mut self) -> Option<(T, Vec<D>)> {
        // println!("calling next");
        let next_key = self.doubles.keys().next().map(|x| x.clone());
        // println!("second step");
        if let Some(key) = next_key {
            // println!("found something");
            self.frontier.update(&key, -1);
            let val = self.doubles.remove(&key).unwrap();
            // println!("returning");
            return Some((key, val));
        }
        else {
            // println!("else");
            self.drain();
            swap(&mut self.buffers, &mut self.doubles);
            // println!("done else");
            return None;
        }
    }
}


impl<T:Timestamp, D:Data> ExchangeReceiver<T, D> {
    fn drain(&mut self) {
        // println!("in drain");
        while let Some((time, data)) = self.receiver.pull() {
            // println!("in drain loop");
            self.consumed.update(&time, (data.len() as i64));
            if !self.buffers.contains_key(&time) { self.frontier.update(&time, 1); self.buffers.insert(time, data); }
            else                                 { self.buffers[time].push_all(data.as_slice()); }
        }
    }

    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>, progress: &mut CountMap<T>) {
        while let Some((ref time, value)) = self.consumed.pop() { consumed.update(time, value); }
        while let Some((ref time, value)) = self.frontier.pop() { progress.update(time, value); }
    }
}
