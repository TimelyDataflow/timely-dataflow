use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::collections::VecDeque;

use progress::Timestamp;
use communication::{Data, Communicator, Pullable, Observer};
use communication::observer::BoxedObserver;

use communication::communicator::thread::Observer as ThreadObserver;
use communication::communicator::thread::Pullable as ThreadPullable;
use communication::observer::exchange::Exchange as ExchangeObserver;

use abomonation::Abomonation;

// A ParallelizationContract transforms the output of a Communicator to an (Observer, Pullable).
pub trait ParallelizationContract<T, D> {
    type Observer: Observer<Time=T, Data=D>+'static;    // 'static bound as boxed by Tee
    type Pullable: Pullable<T, D>+'static;              // 'static bound as boxed by Subgraph
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, Self::Pullable);
}

// direct connection
pub struct Pipeline;
impl<T: Timestamp, D: Data> ParallelizationContract<T, D> for Pipeline {
    type Observer = ThreadObserver<T, D>;
    type Pullable = ThreadPullable<T, D>;
    fn connect<C: Communicator>(self,_communicator: &mut C) -> (Self::Observer, Self::Pullable) {
        let shared = Rc::new(RefCell::new(VecDeque::new()));
        let reverse = Rc::new(RefCell::new(Vec::new()));

        (ThreadObserver::new(shared.clone(), reverse.clone()), ThreadPullable::new(shared.clone(), reverse.clone()))
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

// Exchange uses a Box<Pushable> because it cannot know what type of pushable will return from the communicator.
// The PactObserver will do some buffering for Exchange, cutting down on the virtual calls, but we still
// would like to get the vectors it sends back, so that they can be re-used if possible.
impl<T: Timestamp, D: Data+Abomonation, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    type Observer = ExchangeObserver<BoxedObserver<T, D>, F>;
    type Pullable = Box<Pullable<T, D>>;
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, Self::Pullable) {
        let (senders, receiver) = communicator.new_channel();
        (ExchangeObserver::new(senders, self.hash_func), receiver)
    }
}
