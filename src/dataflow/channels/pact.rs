//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which 
//! creates a pair of `Push` and `Pull` implementors from an `A: Allocate`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::marker::PhantomData;

use timely_communication::{Allocate, Push, Pull, Data};
use timely_communication::allocator::Thread;
use timely_communication::allocator::thread::Pusher as ThreadPusher;
use timely_communication::allocator::thread::Puller as ThreadPuller;

use dataflow::channels::pushers::Exchange as ExchangePusher;
use dataflow::channels::{Message, Content};

use abomonation::Abomonation;

/// A `ParallelizationContract` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContract<T: 'static, D: 'static> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<(T, Content<D>)>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<(T, Content<D>)>+'static;
    /// Alloctes a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Self::Pusher, Self::Puller);
}

/// A direct connection
pub struct Pipeline;
impl<T: 'static, D: 'static> ParallelizationContract<T, D> for Pipeline {
    // TODO: These two could mention types in communication::thread, but they are currently private.
    type Pusher = Pusher<T, D, ThreadPusher<Message<T, D>>>;
    type Puller = Puller<T, D, ThreadPuller<Message<T, D>>>;
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Self::Pusher, Self::Puller) {
        // ignore `&mut A` and use thread allocator
        let (pusher, puller) = Thread::new::<Message<T, D>>();

        (Pusher::new(pusher, allocator.index(), allocator.index(), identifier),
         Puller::new(puller, allocator.index(), identifier))
    }
}

/// An exchange between multiple observers by data
pub struct Exchange<D, F: Fn(&D)->u64+'static> { hash_func: F, phantom: PhantomData<D>, }
impl<D, F: Fn(&D)->u64> Exchange<D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> Exchange<D, F> {
        Exchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Eq+Data+Abomonation, D: Data+Abomonation, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    // TODO: The closure in the type prevents us from naming it. 
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<Push<(T, Content<D>)>>;
    type Puller = Puller<T, D, Box<Pull<Message<T, D>>>>;
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Self::Pusher, Self::Puller) {
        println!("Exchange::connect identifier: {:?} allocate.index: {:?} allocate.peers: {:?}", identifier, allocator.index(), allocator.peers());
        let (senders, receiver) = allocator.allocate::<Message<T, D>>();
        let senders = if let Ok(wmap) = ::std::env::var("WMAP") {
            println!("Got WMAP={:?}", wmap);
            let mut cfg_map: u64 = ::std::u64::MAX;
            for cfg in wmap.split(" ") {
                let id_cfg: Vec<_> = cfg.split(":").collect();
                assert_eq!(id_cfg.len(), 2, "Wrong configuration parameter: {:?}", id_cfg);
                let cfg_id: usize = id_cfg[0].parse().unwrap();
                if cfg_id == identifier {
                    cfg_map = id_cfg[1].parse().unwrap();
                    println!("My WMAP {} -> {}", cfg_id, cfg_map);
                    break;
                }
            }
            assert!(cfg_map.trailing_zeros() < senders.len() as u32, "WMAP would not produce output!");
            senders.into_iter().enumerate()
                .filter(|&(ref i,_)| (1u64 << i & cfg_map) != 0)
                .map(|(i,x)| Pusher::new(x, allocator.index(), i, identifier)).collect::<Vec<_>>()
        } else {
            senders.into_iter().enumerate().map(|(i,x)| Pusher::new(x, allocator.index(), i, identifier)).collect::<Vec<_>>()
        };
        (Box::new(ExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Puller::new(receiver, allocator.index(), identifier))
    }
}

/// An exchange between multiple observers by time and data
pub struct TimeExchange<D, T, F: Fn(&T, &D)->u64+'static> { hash_func: F, phantom: PhantomData<(T, D)>, }
impl<D, T, F: Fn(&T, &D)->u64> TimeExchange<D, T, F> {
    /// Allocates a new `TimeExchange` pact from a distribution function.
    pub fn new(func: F) -> TimeExchange<D, T, F> {
        TimeExchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

impl<T: Eq+Data+Abomonation, D: Data+Abomonation, F: Fn(&T, &D)->u64+'static> ParallelizationContract<T, D> for TimeExchange<D, T, F> {
    type Pusher = ExchangePusher<T, D, Pusher<T, D, Box<Push<Message<T, D>>>>, F>;
    type Puller = Puller<T, D, Box<Pull<Message<T, D>>>>;
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>();
        let senders = senders.into_iter().enumerate().map(|(i,x)| Pusher::new(x, allocator.index(), i, identifier)).collect::<Vec<_>>();
        (ExchangePusher::new(senders, self.hash_func), Puller::new(receiver, allocator.index(), identifier))
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
pub struct Pusher<T, D, P: Push<Message<T, D>>> {
    pusher: P,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
    phantom: ::std::marker::PhantomData<(T, D)>,
}
impl<T, D, P: Push<Message<T, D>>> Pusher<T, D, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, source: usize, target: usize, channel: usize) -> Self {
        Pusher {
            pusher: pusher,
            channel: channel,
            counter: 0,
            source: source,
            target: target,
            phantom: ::std::marker::PhantomData,
        }
    }
}

impl<T, D, P: Push<Message<T, D>>> Push<(T, Content<D>)> for Pusher<T, D, P> {
    #[inline(always)]
    fn push(&mut self, pair: &mut Option<(T, Content<D>)>) {
        if let Some((time, data)) = pair.take() {

            ::logging::log(&::logging::MESSAGES, ::logging::MessagesEvent {
                is_send: true,
                channel: self.channel,
                source: self.source,
                target: self.target,
                seq_no: self.counter,
                length: data.len(),
            });

            let mut message = Some(Message::new(time, data, self.source, self.counter));
            self.counter += 1;
            self.pusher.push(&mut message);
            *pair = message.map(|x| (x.time, x.data));

            // Log something about (index, counter, time?, length?);
        }
        else { self.pusher.done(); }
    }
}

/// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
pub struct Puller<T, D, P: Pull<Message<T, D>>> {
    puller: P,
    current: Option<(T, Content<D>)>,
    channel: usize,
    counter: usize,
    index: usize,
}
impl<T, D, P: Pull<Message<T, D>>> Puller<T, D, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize) -> Self {
        Puller {
            puller: puller,
            channel: channel,
            current: None,
            counter: 0,
            index: index,
        }
    }
}

impl<T, D, P: Pull<Message<T, D>>> Pull<(T, Content<D>)> for Puller<T, D, P> {
    #[inline(always)]
    fn pull(&mut self) -> &mut Option<(T, Content<D>)> {
        let mut previous = self.current.take().map(|(time, data)| Message::new(time, data, self.index, self.counter));
        self.counter += 1;

        ::std::mem::swap(&mut previous, self.puller.pull());

        if let Some(message) = previous.as_ref() {

            ::logging::log(&::logging::MESSAGES, ::logging::MessagesEvent {
                is_send: false,
                channel: self.channel,
                source: message.from,
                target: self.index,
                seq_no: message.seq,
                length: message.data.len(),
            });
        }

        self.current = previous.map(|message| (message.time, message.data));
        &mut self.current
    }
}
