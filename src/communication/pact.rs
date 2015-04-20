use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::Timestamp;
use communication::Data;
use communication::{Communicator, Pullable, Pushable, PushableObserver, Observer};
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
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, Self::Pullable);
}

// direct connection
pub struct Pipeline;
impl<T: Timestamp, D: Data> ParallelizationContract<T, D> for Pipeline {
    type Observer = PushableObserver<T, D, Rc<RefCell<Vec<(T, Vec<D>)>>>>;
    type Pullable = Rc<RefCell<Vec<(T, Vec<D>)>>>;
    fn connect<C: Communicator>(self,_communicator: &mut C) -> (Self::Observer, Self::Pullable) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        return (PushableObserver { data: Vec::new(), pushable: shared.clone(), phantom: PhantomData }, shared);
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
// The PushableObserver will do some buffering for Exchange, cutting down on the virtual calls, but we still
// would like to get the vectors it sends back, so that they can be re-used if possible (given that they are 1:1)
impl<T: Timestamp, D: Data+Columnar, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    type Observer = ExchangeObserver<PushableObserver<T,D,Box<Pushable<(T,Vec<D>)>>>, F>;
    type Pullable = Box<Pullable<(T, Vec<D>)>>;
    fn connect<C: Communicator>(self, communicator: &mut C) -> (Self::Observer, Self::Pullable) {
        let (senders, receiver) = communicator.new_channel();

        let exchange_sender = ExchangeObserver {
            observers:  senders.into_iter().map(|x| PushableObserver::new(x)).collect(),
            hash_func:  self.hash_func,
        };

        return (exchange_sender, receiver);
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
