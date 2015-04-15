use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::Timestamp;
use communication::Data;
use communication::{Communicator, Pullable, Pushable, PushableObserver, Observer};
use communication::observer::ExchangeObserver;

use columnar::Columnar;

// A ParallelizationContract transforms the output of a Communicator, a (Vec<Pushable>, Pullable), to an (Observer, Pullable)
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
    fn connect<C: Communicator>(self,_communicator: &mut C) -> (<Pipeline as ParallelizationContract<T, D>>::Observer,
                                                                <Pipeline as ParallelizationContract<T, D>>::Pullable) {
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

impl<T: Timestamp, D: Data+Columnar, F: Fn(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    type Observer = ExchangeObserver<PushableObserver<T,D,Box<Pushable<(T,Vec<D>)>>>, F>;
    type Pullable = Box<Pullable<(T, Vec<D>)>>;
    fn connect<C: Communicator>(self, communicator: &mut C) -> (<Exchange<D, F> as ParallelizationContract<T, D>>::Observer,
                                                                <Exchange<D, F> as ParallelizationContract<T, D>>::Pullable) {
        let (senders, receiver) = communicator.new_channel();

        let exchange_sender = ExchangeObserver {
            observers:  senders.into_iter().map(|x| PushableObserver { data: Vec::new(), pushable: x, phantom: PhantomData }).collect(),
            hash_func:  self.hash_func,
        };

        return (exchange_sender, receiver);
    }
}

// // broadcasts to all observers
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
