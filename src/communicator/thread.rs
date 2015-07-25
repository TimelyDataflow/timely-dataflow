use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use communicator::{Communicator, Data};
use communicator::pullable::Message;

// The simplest communicator remains worker-local and just queues sent messages.
pub struct Thread;
impl Communicator for Thread {
    fn index(&self) -> u64 { 0 }
    fn peers(&self) -> u64 { 1 }
    fn new_channel<T: Clone+'static, D: 'static>(&mut self) -> (Vec<::observer::BoxedObserver<T, D>>, Box<::communicator::Pullable<T, D>>) {
        let shared = Rc::new(RefCell::new(VecDeque::<(T, Message<D>)>::new()));
        (vec![::observer::BoxedObserver::new(Observer::new(shared.clone()))],
         Box::new(Pullable::new(shared.clone())) as Box<::communicator::Pullable<T, D>>)
    }
}


// an observer wrapping a Rust channel
pub struct Observer<T, D> {
    time: Option<T>,
    dest: Rc<RefCell<VecDeque<(T, Message<D>)>>>,
    // shared: Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T, D> Observer<T, D> {
    pub fn new(dest: Rc<RefCell<VecDeque<(T, Message<D>)>>>) -> Observer<T, D> {
        Observer { time: None, dest: dest }
    }
}

impl<T: Clone, D> ::observer::Observer for Observer<T, D> {
    type Time = T;
    type Data = D;
    #[inline] fn open(&mut self, time: &Self::Time) { assert!(self.time.is_none()); self.time = Some(time.clone()); }
    #[inline] fn shut(&mut self,_time: &Self::Time) { assert!(self.time.is_some()); self.time = None; }
    #[inline] fn give(&mut self, data: &mut Message<Self::Data>) {
        assert!(self.time.is_some());
        // TODO : anything better to do here than allocate (data)?
        // TODO : perhaps team up with the Pushable to recycle (data) ...
        self.dest.borrow_mut().push_back((self.time.clone().unwrap(), ::std::mem::replace(data, Message::Typed(Vec::with_capacity(4096)))));
    }
}

pub struct Pullable<T, D> {
    current: Option<(T, Message<D>)>,
    source: Rc<RefCell<VecDeque<(T, Message<D>)>>>,
    // shared: Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T, D> Pullable<T, D> {
    pub fn new(source: Rc<RefCell<VecDeque<(T, Message<D>)>>>) -> Pullable<T, D> {
        Pullable { current: None, source: source }
    }
}

impl<T, D> ::communicator::pullable::Pullable<T, D> for Pullable<T, D> {
    #[inline]
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> {
        let next = self.source.borrow_mut().pop_front();
        let prev = ::std::mem::replace(&mut self.current, next);
        match prev {
            // TODO : here is where we would recycle data
            Some((_time, Message::Bytes(_bytes, _, _))) => { },
            Some((_time, Message::Typed(_typed))) => { },
            None => { },
        }
        if let Some(ref mut _message) = self.current {
            // TODO : old code; can't recall why this would happen.
            // TODO : probably shouldn't, but I recall a progress
            // TODO : tracking issue if it ever did. check it out!
            // TODO : many operators will call notify_at if they get any messages, is why!
            // assert!(message.look().len() > 0);
        }
        self.current.as_mut().map(|&mut (ref time, ref mut data)| (time, data))
    }
}
