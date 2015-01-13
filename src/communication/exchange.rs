use std::collections::HashMap;
use std::mem::swap;

use progress::Timestamp;
use communication::Data;
use communication::ChannelAllocator;
use communication::Observer;
use communication::observer::{ExchangeObserver};
use std::sync::mpsc::{Sender, Receiver};
use progress::count_map::CountMap;

pub fn exchange_with<T: Timestamp, D: Data, F: Fn(&D) -> u64>(allocator: &mut ChannelAllocator,
                                                             hash_func: F) -> (ExchangeObserver<(Vec<D>, Sender<(T, Vec<D>)>), F>,
                                                                               ExchangeReceiver<T, D>)
{
    let (senders, receiver) = allocator.new_channel();

    let exchange_sender = ExchangeObserver {
        observers:  senders.into_iter().map(|x| (Vec::new(), x)).collect(),
        hash_func:  hash_func,
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
