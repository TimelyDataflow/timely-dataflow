use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::mem::swap;

use progress::Timestamp;
use communication::Data;
use communication::ChannelAllocator;
use communication::Observer;
use communication::observer::{ExchangeObserver};
use std::sync::mpsc::{Sender, Receiver};
use progress::count_map::CountMap;

pub fn exchange_with<T: Timestamp, D: Data, F: Fn(&D) -> u64>(allocator: &mut ChannelAllocator,
                                                             hash_func: F) -> (ExchangeObserver<T, D, (Vec<D>, Sender<(T, Vec<D>)>), F>,
                                                                               ExchangeReceiver<T, D>)
{
    let (senders, receiver) = allocator.new_channel();

    let exchange_sender = ExchangeObserver {
        observers:  senders.into_iter().map(|x| (Vec::new(), x)).collect(),
        hash_func:  hash_func,
        // degree:     allocator.multiplicity(),
        // buffers:    HashMap::new(),
        // senders:    senders,
    };

    let exchange_receiver = ExchangeReceiver {
        receiver:   receiver,
        buffers:    HashMap::new(),
        doubles:    HashMap::new(),
        consumed:   Vec::new(),
        frontier:   Vec::new(),
    };

    return (exchange_sender, exchange_receiver);
}

// pub struct ExchangeSender<T:Timestamp, D:Data, O: Observer<(T, Vec<D>)>, F: Fn(&D) -> u64> {
//     degree:     uint,
//     buffers:    HashMap<T, Vec<Vec<D>>>, // one row of buffers for each time
//     senders:    Vec<O>,
//     hash_func:  F,
// }
//
// impl<T:Timestamp, D:Data, O: Observer<(T, Vec<D>)>, F: Fn(&D) -> u64+'static> Observer<(T, Vec<D>)> for ExchangeSender<T, D, O, F> {
//     fn next(&mut self, (time, data): (T, Vec<D>)) -> () {
//         let array = match self.buffers.entry(&time) {
//             Occupied(x) => x.into_mut(),
//             Vacant(x)   => x.insert(range(0, self.degree).map(|_| Vec::new()).collect()),
//         };
//
//         for datum in data.into_iter() { array[((self.hash_func)(&datum) % self.degree as u64) as uint].push(datum); }
//         for index in range(0, array.len()) {
//             if array[index].len() > 256 {
//                 self.senders[index].next((time, array[index].clone()));
//                 array[index].clear();
//             }
//         }
//     }
//
//     fn done(&mut self) -> () {
//         for (time, array) in self.buffers.iter() {
//             for index in range(0, array.len()) {
//                 self.senders[index].next((*time, array[index].clone()));
//             }
//         }
//
//         for sender in self.senders.iter_mut() { sender.done(); }
//
//         self.buffers.clear();
//     }
// }

pub struct ExchangeReceiver<T:Timestamp, D:Data>
{
    receiver:   Receiver<(T, Vec<D>)>,  // receiver pair for the exchange channel
    buffers:    HashMap<T, Vec<D>>,     // buffers incoming records indexed by time
    doubles:    HashMap<T, Vec<D>>,     // double-buffered to prevent unbounded reading

    consumed:   Vec<(T, i64)>,          // retains cumulative messages consumed
    frontier:   Vec<(T, i64)>,          // retains un-claimed messages updates
}

impl<T:Timestamp, D:Data> Iterator for ExchangeReceiver<T, D>
{
    type Item = (T, Vec<D>);

    fn next(&mut self) -> Option<(T, Vec<D>)> {
        let next_key = self.doubles.keys().next().map(|x| x.clone());
        if let Some(key) = next_key {
            self.frontier.update(&key, -1);
            let val = self.doubles.remove(&key).unwrap();
            return Some((key, val));
        }
        else {
            self.drain();
            swap(&mut self.buffers, &mut self.doubles);
            return None;
        }
    }
}


impl<T:Timestamp, D:Data> ExchangeReceiver<T, D>
{
    fn drain(&mut self) {
        while let Ok((time, data)) = self.receiver.try_recv() {
            self.consumed.update(&time, (data.len() as i64));
            if !self.buffers.contains_key(&time) { self.frontier.update(&time, 1); self.buffers.insert(time, data); }
            else                                 { self.buffers[time].push_all(data.as_slice()); }
        }
    }

    pub fn pull_progress(&mut self, consumed: &mut Vec<(T, i64)>, progress: &mut Vec<(T, i64)>) {
        while let Some((ref time, value)) = self.consumed.pop() { consumed.update(time, value); }
        while let Some((ref time, value)) = self.frontier.pop() { progress.update(time, value); }
    }
}
