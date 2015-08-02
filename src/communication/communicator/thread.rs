use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use communication::{Communicator, Data, Message};

// The simplest communicator remains worker-local and just queues sent messages.
pub struct Thread;
impl Communicator for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn new_channel<T: Clone+'static, D: 'static>(&mut self) ->
            (Vec<::communication::observer::BoxedObserver<T, D>>,
             Box<::communication::Pullable<T, D>>) {
        let shared = Rc::new(RefCell::new(VecDeque::<(T, Message<D>)>::new()));
        let reverse = Rc::new(RefCell::new(Vec::new()));

        (vec![::communication::observer::BoxedObserver::new(Observer::new(shared.clone(), reverse.clone()))],
         Box::new(Pullable::new(shared, reverse)) as Box<::communication::Pullable<T, D>>)
    }
}


// an observer wrapping a Rust channel
pub struct Observer<T, D> {
    time: Option<T>,
    dest: Rc<RefCell<VecDeque<(T, Message<D>)>>>,
    reverse: Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T, D> Observer<T, D> {
    pub fn new(dest: Rc<RefCell<VecDeque<(T, Message<D>)>>>, reverse: Rc<RefCell<Vec<Vec<D>>>>) -> Observer<T, D> {
        Observer { time: None, dest: dest, reverse: reverse }
    }
}

impl<T: Clone, D> ::communication::observer::Observer for Observer<T, D> {
    type Time = T;
    type Data = D;
    #[inline] fn open(&mut self, time: &Self::Time) { assert!(self.time.is_none()); self.time = Some(time.clone()); }
    #[inline] fn shut(&mut self,_time: &Self::Time) { assert!(self.time.is_some()); self.time = None; }
    #[inline] fn give(&mut self, data: &mut Message<Self::Data>) {
        assert!(self.time.is_some());
        // ALLOC : We replace with empty typed data. Not clear why typed is better than binary,
        // ALLOC : but lots of folks wouldn't expect bytes, and anyone should tolerate typed.
        if data.len() > 0 {
            let mut replacement = self.reverse.borrow_mut().pop().unwrap_or(Vec::new());
            let to_push = ::std::mem::replace(data, Message::from_typed(&mut replacement));
            self.dest.borrow_mut().push_back((self.time.clone().unwrap(), to_push));
        }
    }
}

pub struct Pullable<T, D> {
    current: Option<(T, Message<D>)>,
    source: Rc<RefCell<VecDeque<(T, Message<D>)>>>,
    reverse: Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T, D> Pullable<T, D> {
    pub fn new(source: Rc<RefCell<VecDeque<(T, Message<D>)>>>, reverse: Rc<RefCell<Vec<Vec<D>>>>) -> Pullable<T, D> {
        Pullable { current: None, source: source, reverse: reverse }
    }
}

impl<T, D> ::communication::pullable::Pullable<T, D> for Pullable<T, D> {
    #[inline]
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> {

        // ALLOC : Here is one place where we drop both binary and typed data.
        // ALLOC : Ideally the lazy re-allocation elsewhere means we don't always drop.
        if let Some((_time, data)) = self.current.take() {
            let mut vector = data.into_typed();
            if vector.capacity() == Message::<D>::default_length() {
                vector.clear();
                // self.reverse.borrow_mut().push(vector);
            }
        }

        self.current = self.source.borrow_mut().pop_front();

        if let Some((_, ref message)) = self.current {
            // should be ensured by test in Thread::Observer.give().
            assert!(message.len() > 0);
        }
        self.current.as_mut().map(|&mut (ref time, ref mut data)| (time, data))
    }
}
