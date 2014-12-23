use std::collections::HashMap;
use std::collections::hash_map::{Occupied, Vacant};
use std::mem::swap;

use progress::Timestamp;
use communication::Data;
use communication::ChannelAllocator;
use example::ports::TargetPort;
use progress::count_map::CountMap;

pub fn exchange_with<T: Timestamp, D: Data>(allocator: &mut ChannelAllocator,
                                            hash_func: |D|:'static -> u64) -> (ExchangeSender<T, D>, ExchangeReceiver<T, D>)
{
    let (senders, receiver) = allocator.new_channel();

    let exchange_sender = ExchangeSender
    {
        degree:     allocator.multiplicity(),
        buffers:    HashMap::new(),
        senders:    senders,
        hash_func:  hash_func,
    };

    let exchange_receiver = ExchangeReceiver
    {
        receiver:   receiver.unwrap(),
        buffers:    HashMap::new(),
        doubles:    HashMap::new(),
        consumed:   Vec::new(),
        frontier:   Vec::new(),
    };

    return (exchange_sender, exchange_receiver);
}

pub struct ExchangeSender<T:Timestamp, D:Data>
{
    degree:     uint,
    buffers:    HashMap<T, Vec<Vec<D>>>, // one row of buffers for each time
    senders:    Vec<Sender<(T, Vec<D>)>>,
    hash_func:  |D|:'static -> u64,
}

impl<T:Timestamp, D:Data> TargetPort<T, D> for ExchangeSender<T, D>
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>) -> ()
    {
        let array = match self.buffers.entry(*time)
        {
            Occupied(x) => x.into_mut(),
            Vacant(x)   => x.set(Vec::from_fn(self.degree, |_| Vec::new())),
        };

        for &datum in data.iter()
        {
            array[((self.hash_func)(datum) % self.degree as u64) as uint].push(datum);
        }

        for index in range(0, array.len())
        {
            if array[index].len() > 256
            {
                self.senders[index].send((*time, array[index].clone()));
                array[index].clear();
            }
        }
    }

    fn flush(&mut self) -> ()
    {
        for (time, array) in self.buffers.iter()
        {
            for index in range(0, array.len())
            {
                self.senders[index].send((*time, array[index].clone()));
            }
        }

        self.buffers.clear();
    }
}

pub struct ExchangeReceiver<T:Timestamp, D:Data>
{
    receiver:   Receiver<(T, Vec<D>)>,  // receiver pair for the exchange channel
    buffers:    HashMap<T, Vec<D>>,     // buffers incoming records indexed by time
    doubles:    HashMap<T, Vec<D>>,     // double-buffered to prevent unbounded reading

    consumed:   Vec<(T, i64)>,          // retains cumulative messages consumed
    frontier:   Vec<(T, i64)>,          // retains un-claimed messages updates
}

impl<T:Timestamp, D:Data> Iterator<(T,Vec<D>)> for ExchangeReceiver<T, D>
{
    fn next(&mut self) -> Option<(T, Vec<D>)>
    {
        // if data in double-buffer, return it. else swap buffers.
        if let Some(key) = self.doubles.keys().next().map(|&x|x)
        {
            self.frontier.update(key, -1);
            return self.doubles.remove(&key).map(|x| (key, x));
        }
        else
        {
            self.drain();
            swap(&mut self.buffers, &mut self.doubles);
            return None;
        }
    }
}


impl<T:Timestamp, D:Data> ExchangeReceiver<T, D>
{
    fn drain(&mut self)
    {
        while let Ok((time, data)) = self.receiver.try_recv()
        {
            self.consumed.update(time, (data.len() as i64));

            if !self.buffers.contains_key(&time) { self.buffers.insert(time, data); self.frontier.update(time, 1); }
            else                                 { self.buffers[time].push_all(data.as_slice()); }
        }
    }

    pub fn pull_progress(&mut self, consumed: &mut Vec<(T, i64)>, progress: &mut Vec<(T, i64)>)
    {
        while let Some((time, value)) = self.consumed.pop() { consumed.update(time, value); }
        while let Some((time, value)) = self.frontier.pop() { progress.update(time, value); }
    }
}
