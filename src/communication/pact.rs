// use std::rc::Rc;
// use std::cell::RefCell;
use std::marker::PhantomData;
// use std::collections::VecDeque;

use fabric::{Allocate, Push, Pull, Data};
use fabric::allocator::Thread;

// use progress::Timestamp;
use communication::observer::exchange::Exchange as ExchangeObserver;
use communication::message::{Message, Content};

use abomonation::Abomonation;

// A ParallelizationContract transforms the output of a Allocate to an (Observer, Pullable).
pub trait ParallelizationContract<T: Data, D: Data> {
    fn connect<A: Allocate>(self, allocator: &mut A) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>);
}

// direct connection
pub struct Pipeline;
// TODO : +Abomonation probably not needed?
impl<T: Data+Abomonation, D: Data+Abomonation> ParallelizationContract<T, D> for Pipeline {
    fn connect<A: Allocate>(self, _: &mut A) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        // ignore &mut A and use thread allocator
        let (mut pushers, puller) = Thread.allocate::<Message<T, D>>();
        (Box::new(Pusher { pusher: pushers.pop().unwrap() }),
         Box::new(Puller { puller: puller, current: None } ))
    }
}

// exchanges between multiple observers
pub struct Exchange<D, F: Fn(&D)->u64+'static> { hash_func: F, phantom: PhantomData<D>, }
impl<D, F: Fn(&D)->u64> Exchange<D, F> {
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
    fn connect<A: Allocate>(self, allocator: &mut A) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        let (senders, receiver) = allocator.allocate();
        let senders = senders.into_iter().map(|x| Pusher { pusher: x} ).collect::<Vec<_>>();
        (Box::new(ExchangeObserver::new(senders, self.hash_func)), Box::new(Puller { puller: receiver, current: None }))
    }
}

pub struct Pusher<T, D> {
    pusher: Box<Push<Message<T, D>>>
}

impl<T, D> Push<(T, Content<D>)> for Pusher<T, D> {
    fn push(&mut self, pair: &mut Option<(T, Content<D>)>) {
        if let Some((time, data)) = pair.take() {
            let mut message = Some(Message { time: time, data: data });
            self.pusher.push(&mut message);
            *pair = message.map(|x| (x.time, x.data));
        }
        else { self.pusher.done(); }
    }
}

pub struct Puller<T, D> {
    puller: Box<Pull<Message<T, D>>>,
    current: Option<(T, Content<D>)>,
}

impl<T, D> Pull<(T, Content<D>)> for Puller<T, D> {
    fn pull(&mut self) -> &mut Option<(T, Content<D>)> {
        let mut previous = self.current.take().map(|(time, data)| Message { time: time, data: data});
        ::std::mem::swap(&mut previous, self.puller.pull());
        self.current = previous.map(|message| (message.time, message.data));
        &mut self.current
    }
}
