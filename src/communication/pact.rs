use std::mem;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::collections::VecDeque;

use progress::Timestamp;
use communication::Data;
use communication::{Communicator, Pullable, Pushable, Observer};
use communication::observer::ExchangeObserver;

use abomonation::Abomonation;

// A ParallelizationContract transforms the output of a Communicator, a (Vec<Pushable>, Pullable),
// to an (Observer, Pullable).
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
impl<T: Timestamp, D: Data+Abomonation, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    type Observer = ExchangeObserver<PactObserver<T, D, Box<Pushable<(T, Vec<D>)>>>, F>;
    type Pullable = Box<Pullable<(T, Vec<D>)>>;
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, PactPullable<T, D, Self::Pullable>) {
        let (senders, receiver) = communicator.new_channel();
        let shared = Rc::new(RefCell::new(Vec::new()));
        let exchange_sender = ExchangeObserver::new(senders.into_iter().map(|x| PactObserver::new(x, shared.clone())).collect(), self.hash_func);
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
    // this method is repeatedly called by worker logic.
    // we want to clean up prior data in any way needed,
    // for example recycling any remaining elements and
    // pushing the buffer into self.shared.
    pub fn pull(&mut self) -> Option<(T, &mut Vec<D>)> {
        assert!(self.buffer.len() == 0);                // TODO : this is pretty hard core ...
        if let Some((time, mut data)) = self.pullable.pull() {
            if data.len() > 0 {
                mem::swap(&mut data, &mut self.buffer); // install buffer
                // if data.capacity() == 2048 {
                //     self.shared.borrow_mut().push(data);    // TODO : determine sane buffering strategy
                // }
                Some((time, &mut self.buffer))
            } else { None }
        } else { None }
    }
    fn new(pullable: P, shared: Rc<RefCell<Vec<Vec<D>>>>) -> PactPullable<T, D, P> {
        PactPullable {
            pullable: pullable,
            buffer:   Vec::with_capacity(2048),
            shared:   shared,
            phantom:  PhantomData,
        }
    }
}

pub struct PactObserver<T:Send, D:Send+Clone, P: Pushable<(T, Vec<D>)>> {
    pub pushable:   P,
    shared:         Rc<RefCell<Vec<Vec<D>>>>,
    current_time:   Option<T>,
}

impl<T:Send, D:Send+Clone, P: Pushable<(T, Vec<D>)>> PactObserver<T, D, P> {
    pub fn new(pushable: P, shared: Rc<RefCell<Vec<Vec<D>>>>) -> PactObserver<T, D, P> {
        PactObserver {
            pushable: pushable,
            shared: shared,
            current_time: None,
        }
    }
}

impl<T:Send+Clone, D:Send+Clone, P: Pushable<(T, Vec<D>)>> Observer for PactObserver<T,D,P> {
    type Time = T;
    type Data = D;
    #[inline] fn open(&mut self, time: &T) {
        assert!(self.current_time.is_none());
        self.current_time = Some(time.clone());
    }
    #[inline] fn shut(&mut self,_time: &T) {
        assert!(self.current_time.is_some());
        self.current_time = None;

        // we've just been shut; if there are spare shared buffers, we might return some of them
        if self.shared.borrow().len() > 0 {
            let len = self.shared.borrow().len();
            self.shared.borrow_mut().truncate(len / 2);
        }
    }
    #[inline] fn give(&mut self, data: &mut Vec<D>) {
        if let Some(time) = self.current_time.clone() {
            let empty = self.shared.borrow_mut().pop().unwrap_or_else(|| Vec::with_capacity(2048));
            assert!(empty.len() == 0);
            self.pushable.push((time, mem::replace(data, empty)));
        }
        else {
            panic!("PactObserver: self.give() with unset self.current_time");
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
