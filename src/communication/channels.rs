use std::collections::HashMap;
use std::collections::hash_map::{Occupied, Vacant};
use core::fmt::Show;

use std::sync::{Arc, Mutex};
use std::any::{Any, AnyMutRefExt};

use progress::Timestamp;
use progress::count_map::CountMap;

use example::ports::TargetPort;
use example::ports::TeePort;

pub trait Data : Copy+Clone+Send+Show+'static { }

impl<T: Copy+Clone+Send+Show+'static> Data for T { }



pub struct ChannelAllocator
{
    index:              uint,
    pub multiplicity:   uint,
    allocated:          uint,                             // indicates how many have been allocated (locally).
    channels:           Arc<Mutex<Vec<Box<Any+Send>>>>,   // type -> Vec<(Vec<Sender<type>>, Vec<Receiver<type>>)>
}

impl ChannelAllocator
{
    pub fn new_channel<T:Send>(&mut self) -> (Vec<Sender<T>>, Option<Receiver<T>>)
    {
        let mut channels = self.channels.lock();

        // we need a new channel ...
        if self.allocated == channels.len()
        {
            let mut senders = Vec::new();
            let mut receivers = Vec::new();

            for _ in range(0, self.multiplicity)
            {
                let (s, r): (Sender<T>, Receiver<T>) = channel();
                senders.push(s);
                receivers.push(Some(r));
            }

            channels.push(box() (senders, receivers));
        }

        match channels[self.allocated].downcast_mut::<(Vec<Sender<T>>, Vec<Option<Receiver<T>>>)>()
        {
            Some(&(ref mut senders, ref mut receivers)) =>
            {
                self.allocated += 1;
                (senders.clone(), receivers[self.index].take())
            }
            _ => { panic!("unable to cast channel correctly"); }
        }
    }

    pub fn new_vector(multiplicity: uint) -> Vec<ChannelAllocator>
    {
        let channels = Arc::new(Mutex::new(Vec::new()));

        Vec::from_fn(multiplicity, |index| ChannelAllocator
            {
                index: index,
                multiplicity: multiplicity,
                allocated: 0,
                channels: channels.clone(),
                })
            }
        }

pub trait RecvPort<T: Timestamp, D: Data> : Iterator<(T, Vec<D>)>+'static
{
    //fn recv_at(&mut self, time: &T) -> Vec<D>;
}

pub trait SendPort<T: Timestamp, D: Data> : 'static
{
    fn send_at(&mut self, time: &T, data: &Vec<D>) -> ();
}


pub struct OutputBufferHelper<'a, T: Timestamp, D: Data>
{
    parent: &'a mut OutputBuffer<T, D>,

    time:   T,
    buffer: Vec<D>,
}

impl<'a, T: Timestamp, D: Data> OutputBufferHelper<'a, T, D>
{
    #[inline(always)]
    pub fn send(&mut self, data: D) -> ()
    {
        self.buffer.push(data);
        if self.buffer.len() >= 256
        {
            self.flush();
        }
    }

    pub fn flush(&mut self)
    {
        if self.buffer.len() > 0
        {
            self.parent.send_at(&mut self.buffer, &self.time);
            self.buffer.clear();
        }
    }
}

pub struct OutputBuffer<T: Timestamp, D: Data>
{
    pub buffers:    HashMap<T, Vec<D>>,
    pub target:     TeePort<T, D>,
}

impl<T:Timestamp, D:Data> OutputBuffer<T, D>
{
    pub fn send_at(&mut self, data: &mut Vec<D>, time: &T)
    {
        let buffer = match self.buffers.entry(*time)
        {
            Occupied(x) => x.into_mut(),
            Vacant(x)   => x.set(Vec::new()),
        };

        buffer.push_all(data.as_slice());

        // TODO: should actually send, if the buffer is getting full.
    }

    pub fn buffer_for<'a>(&'a mut self, time: &T) -> OutputBufferHelper<'a, T, D>
    {
        OutputBufferHelper
        {
            parent: self,
            time:   *time,
            buffer: Vec::new(),
        }
    }

    pub fn flush(&mut self)
    {
        for (time, data) in self.buffers.iter()
        {
            self.target.deliver_data(time, data);
        }

        self.buffers.clear();
        self.target.flush();
    }

    pub fn pull_progress(&mut self, updates: &mut Vec<(T, i64)>)
    {
        let mut borrowed = self.target.updates.borrow_mut();

        while let Some(update) = borrowed.pop() { updates.push(update); }
    }
}



impl<T:Timestamp, D:Data> SendPort<T, D> for Sender<(T, Vec<D>)>
{
    fn send_at(&mut self, time: &T, data: &Vec<D>) -> ()
    {
        self.send((*time, data.clone()));
    }
}



pub fn exchange_with<T: Timestamp, D: Data>(allocator: &mut ChannelAllocator, hash_func: |D|:'static -> uint) ->
    (ExchangeSender<T, D>, ExchangeReceiver<T, D>)
{
    let (senders, receiver) = allocator.new_channel();

    let exchange_sender = ExchangeSender
    {
        degree:     allocator.multiplicity,
        buffers:    HashMap::new(),
        senders:    senders,
        hash_func:  hash_func,
    };

    let exchange_receiver = ExchangeReceiver
    {
        buffers:    HashMap::new(),
        receiver:   receiver.unwrap(),
        consumed:    Vec::new(),
        frontier:    Vec::new(),
    };

    return (exchange_sender, exchange_receiver);
}

pub struct ExchangeSender<T:Timestamp, D:Data>
{
    degree:     uint,
    buffers:    HashMap<T, Vec<Vec<D>>>, // one row of buffers for each time
    senders:    Vec<Sender<(T, Vec<D>)>>,
    hash_func:  |D|:'static -> uint,
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
            // println!("recv'd data {} at time {}", datum, time);
            array[(self.hash_func)(datum) % self.degree].push(datum);
        }

        for index in range(0, array.len())
        {
            //if array[index].len() > 256
            {
                self.senders[index].send_at(time, &array[index].clone());
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
                self.senders[index].send_at(time, &array[index]);
            }
        }

        self.buffers.clear();
    }
}

pub struct ExchangeReceiver<T:Timestamp, D:Data>
{
    buffers:    HashMap<T, Vec<D>>,
    receiver:   Receiver<(T, Vec<D>)>,

    consumed:   Vec<(T, i64)>,
    frontier:   Vec<(T, i64)>,
}

impl<T:Timestamp, D:Data> RecvPort<T,D> for ExchangeReceiver<T, D>
{
    /*
    fn recv_at(&mut self, time: &T) -> Vec<D>
    {
        self.drain();

        if let Some(data) = self.buffers.remove(time)
        {
            self.frontier.update(*time, -1);
            return data;
        }
        else
        {
            return Vec::new();
        }
    }
    */
}

impl<T:Timestamp, D:Data> Iterator<(T,Vec<D>)> for ExchangeReceiver<T, D>
{
    fn next(&mut self) -> Option<(T, Vec<D>)>
    {
        self.drain();
        if let Some(key) = self.buffers.keys().next().map(|&x|x)
        {
            self.frontier.update(key, -1);
            return self.buffers.remove(&key).map(|x| (key, x));
        }
        else
        {
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
