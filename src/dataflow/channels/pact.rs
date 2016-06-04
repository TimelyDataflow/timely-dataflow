//! Parallelization contracts, describing requirements for data movement along dataflow edges.

use std::marker::PhantomData;

use timely_communication::{Allocate, Push, Pull, Data};
use timely_communication::allocator::Thread;

use dataflow::channels::pushers::Exchange as ExchangeObserver;
use dataflow::channels::{Message, Content};

use abomonation::Abomonation;

/// A ParallelizationContract transforms the output of a Allocate to an (Observer, Pullable).
pub trait ParallelizationContract<T: 'static, D: 'static> {
    /// Alloctes a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>);
}

/// A direct connection
pub struct Pipeline;
impl<T: 'static, D: 'static> ParallelizationContract<T, D> for Pipeline {
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        // ignore &mut A and use thread allocator
        let (mut pushers, puller) = Thread::new::<Message<T, D>>();

        (Box::new(Pusher::new(pushers.pop().unwrap(), allocator.index(), allocator.index(), identifier)),
         Box::new(Puller::new(puller, allocator.index(), identifier)))
    }
}

/// An exchange between multiple observers
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

// Exchange uses a Box<Pushable> because it cannot know what type of pushable will return from the allocator.
// The PactObserver will do some buffering for Exchange, cutting down on the virtual calls, but we still
// would like to get the vectors it sends back, so that they can be re-used if possible.
impl<T: Eq+Data+Abomonation, D: Data+Abomonation, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        let (senders, receiver) = allocator.allocate::<Message<T, D>>();
        let senders = senders.into_iter().enumerate().map(|(i,x)| Pusher::new(x, allocator.index(), i, identifier)).collect::<Vec<_>>();
        (Box::new(ExchangeObserver::new(senders, self.hash_func)), Box::new(Puller::new(receiver, allocator.index(), identifier)))
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
pub struct Pusher<T, D> {
    pusher: Box<Push<Message<T, D>>>,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
}
impl<T, D> Pusher<T, D> {
    /// Allocates a new pusher.
    pub fn new(pusher: Box<Push<Message<T, D>>>, source: usize, target: usize, channel: usize) -> Pusher<T, D> {
        Pusher {
            pusher: pusher,
            channel: channel,
            counter: 0,
            source: source,
            target: target,
        }
    }
}

impl<T, D> Push<(T, Content<D>)> for Pusher<T, D> {
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
pub struct Puller<T, D> {
    puller: Box<Pull<Message<T, D>>>,
    current: Option<(T, Content<D>)>,
    channel: usize,
    counter: usize,
    index: usize,
}
impl<T, D> Puller<T, D> {
    /// Allocates a new `Puller`.
    pub fn new(puller: Box<Pull<Message<T, D>>>, index: usize, channel: usize) -> Puller<T, D> {
        Puller {
            puller: puller,
            channel: channel,
            current: None,
            counter: 0,
            index: index,
        }
    }
}

impl<T, D> Pull<(T, Content<D>)> for Puller<T, D> {
    fn pull(&mut self) -> &mut Option<(T, Content<D>)> {
        let mut previous = self.current.take().map(|(time, data)| Message::new(time, data, self.index, self.counter));
        self.counter += 1;

        ::std::mem::swap(&mut previous, self.puller.pull());

        if let Some(ref message) = previous.as_ref() {

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
