use std::mem;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::collections::VecDeque;

use progress::Timestamp;
use communication::Data;
use communication::{Communicator, Pullable, Pushable, Observer};
use communication::observer::ExchangeObserver;

use columnar::Columnar;

// A ParallelizationContract transforms the output of a Communicator, a (Vec<Pushable>, Pullable), to an (Observer, Pullable)
// TODO : There is an asymmetry with the Observer/Pullable interface at the moment in that the Pullable
// TODO : yields (and loses) vectors, which need to be continually allocated on the Observer side. It would
// TODO : be better if there were a way to recycle them, perhaps by having the Pullable only show them,
// TODO : or be a Pullable<(&T, &mut Vec<D>)>, or Pullable<(T, Iterator<D>)>, or who knows what...
pub trait ParallelizationContract<T: Timestamp, D: Data> {
    type Observer: Observer<Time=T, Data=D>+'static;
    type Pullable: Pullable<(T, Vec<D>)>+'static;
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, PactPullable<T, D, Self::Pullable>);
}

// direct connection
pub struct Pipeline;
impl<T: Timestamp, D: Data> ParallelizationContract<T, D> for Pipeline {
    type Observer = PactObserver<T, D, Rc<RefCell<VecDeque<(T, Vec<D>)>>>>;
    type Pullable = Rc<RefCell<VecDeque<(T, Vec<D>)>>>;
    fn connect<C: Communicator>(self,_communicator: &mut C) -> (Self::Observer, PactPullable<T, D, Self::Pullable>) {
        let shared1 = Rc::new(RefCell::new(VecDeque::new()));
        let shared2 = Rc::new(RefCell::new(Vec::new()));
        return (PactObserver::new(shared1.clone(), shared2.clone()), PactPullable::new(shared1, shared2));
    }
}

// exchanges between multiple observers
pub struct Exchange<D, F: Fn(&D)->u64> { hash_func: F, phantom: PhantomData<D>, }
impl<D, F: Fn(&D)->u64> Exchange<D, F> {
    pub fn new(func: F) -> Exchange<D, F> {
        Exchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a Box<Pushable> because it cannot know what type of pushable will return from the communicator.
// The PactObserver will do some buffering for Exchange, cutting down on the virtual calls, but we still
// would like to get the vectors it sends back, so that they can be re-used if possible.
impl<T: Timestamp, D: Data+Columnar, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    type Observer = ExchangeObserver<PactObserver<T, D, Box<Pushable<(T, Vec<D>)>>>, F>;
    type Pullable = Box<Pullable<(T, Vec<D>)>>;
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, PactPullable<T, D, Self::Pullable>) {
        let (senders, receiver) = communicator.new_channel();

        let shared = Rc::new(RefCell::new(Vec::new()));

        let exchange_sender = ExchangeObserver {
            observers:  senders.into_iter().map(|x| PactObserver::new(x, shared.clone())).collect(),
            hash_func:  self.hash_func,
        };

        return (exchange_sender, PactPullable::new(receiver, shared));
    }
}

pub struct PactPullable<T:Send, D:Send+Clone, P: Pullable<(T, Vec<D>)>> {
    pullable: P,
    buffer:   Vec<D>,
    shared:   Rc<RefCell<Vec<Vec<D>>>>,
    phantom:  PhantomData<T>,
}

impl<T:Send, D: Send+Clone, P: Pullable<(T, Vec<D>)>> PactPullable<T, D, P> {
    pub fn pull(&mut self) -> Option<(T, &mut Vec<D>)> {
        if let Some((time, mut data)) = self.pullable.pull() {
            if data.len() > 0 {
                mem::swap(&mut data, &mut self.buffer);
                data.clear();
                // self.shared.borrow_mut().push(data);
                Some((time, &mut self.buffer))
            } else { None }
        } else { None }
    }
    fn new(pullable: P, shared: Rc<RefCell<Vec<Vec<D>>>>) -> PactPullable<T, D, P> {
        PactPullable {
            pullable: pullable,
            buffer:   Vec::new(),
            shared:   shared,
            phantom:  PhantomData,
        }
    }
}

pub struct PactObserver<T:Send, D:Send+Clone, P: Pushable<(T, Vec<D>)>> {
    pub data:       Vec<D>,
    pub pushable:   P,
    pub phantom:    PhantomData<T>,
    shared:         Rc<RefCell<Vec<Vec<D>>>>,
    // threshold:      usize, // TODO : add this in, cloning time, I guess.
}

impl<T:Send, D:Send+Clone, P: Pushable<(T, Vec<D>)>> PactObserver<T, D, P> {
    pub fn new(pushable: P, shared: Rc<RefCell<Vec<Vec<D>>>>) -> PactObserver<T, D, P> {
        PactObserver {
            data:       Vec::new(),
            pushable:   pushable,
            phantom:    PhantomData,
            shared:     shared,
        }
    }
}

impl<T:Send+Clone, D:Send+Clone, P: Pushable<(T, Vec<D>)>> Observer for PactObserver<T,D,P> {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self,_time: &T) { }
    #[inline(always)] fn show(&mut self, data: &D) { self.give(data.clone()); }
    #[inline(always)] fn give(&mut self, data:  D) { self.data.push(data); }
    #[inline(always)] fn shut(&mut self, time: &T) {
        if self.data.len() > 0 {
            let empty = self.shared.borrow_mut().pop().unwrap_or(Vec::new());
            // if empty.capacity() == 0 { println!("empty buffer!"); }
            self.pushable.push((time.clone(), mem::replace(&mut self.data, empty)));
        }
    }
}


// broadcasts to all observers
// TODO : This needs support from the progress tracking protocol;
// TODO : we don't know/report how many messages are actually created.
// pub struct Broadcast;
// impl<T: Timestamp, D: Data+Columnar> ParallelizationContract<T, D> for Broadcast {
//     type Observer = BroadcastObserver<PushableObserver<T,D,Box<Pushable<(T,Vec<D>)>>>>;
//     type Pullable = Box<Pullable<(T, Vec<D>)>>;
//     fn connect<C: Communicator>(self, communicator: &mut C) -> (<Broadcast as ParallelizationContract<T, D>>::Observer,
//                                                                 <Broadcast as ParallelizationContract<T, D>>::Pullable) {
//         let (senders, receiver) = communicator.new_channel();
//
//         let exchange_sender = BroadcastObserver {
//             observers:  senders.into_iter().map(|x| PushableObserver { data: Vec::new(), pushable: x, phantom: PhantomData }).collect()
//         };
//
//         panic!("Broadcast won't work until progress counting becomes aware of how many records are sent");
//
//         return (exchange_sender, receiver);
//     }
// }
