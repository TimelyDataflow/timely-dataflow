use std::mem;
use std::rc::Rc;
use std::cell::RefCell;

use progress::Timestamp;
use communication::{Data, Observer};

// half of output_port for observing data
pub struct OutputPort<T: Timestamp, D: Data> {
    limit:  usize,
    buffer: Vec<D>,
    spare:  Vec<D>,
    shared: Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>>,
}

impl<T: Timestamp, D: Data> Observer for OutputPort<T, D> {
    type Time = T;
    type Data = D;

    #[inline(always)]
    fn open(&mut self, time: &T) {
        for observer in self.shared.borrow_mut().iter_mut() { observer.open(time); }
    }

    #[inline(always)] fn shut(&mut self, time: &T) {
        if self.buffer.len() > 0 { self.flush_buffer(); }
        for observer in self.shared.borrow_mut().iter_mut() { observer.shut(time); }
    }
    #[inline(always)] fn give(&mut self, data: &mut Vec<D>) {
        if self.buffer.len() > 0 { self.flush_buffer(); }
        mem::swap(&mut self.buffer, data);
        if self.buffer.len() >= self.limit { self.flush_buffer(); }
    }
}

impl<T: Timestamp, D: Data> OutputPort<T, D> {
    pub fn new() -> (OutputPort<T, D>, Registrar<T, D>) {
        let limit = 256; // TODO : Used to be a parameter, but not clear that the user should
                         // TODO : need to know the right value here. Think a bit harder...

        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = OutputPort {
            limit:  limit,
            buffer: Vec::with_capacity(limit),
            spare:  Vec::with_capacity(limit),
            shared: shared.clone(),
        };

        (port, Registrar { shared: shared })
    }
    #[inline(always)]
    fn flush_buffer(&mut self) {
        let mut observers = self.shared.borrow_mut();

        // at the beginning of each iteration, self.buffer is valid and self.spare is empty.
        for index in (0..observers.len()) {
            mem::swap(&mut self.buffer, &mut self.spare); // spare valid, buffer empty
            // TODO : was push_all, now extend. currently extend is slow. watch.
            if index < observers.len() - 1 { self.buffer.extend(self.spare.iter().cloned()); }
            observers[index].give(&mut self.spare);
            self.spare.clear();
        }
        self.buffer.clear(); // in case observers.len() == 0
    }
}

impl<T: Timestamp, D: Data> Clone for OutputPort<T, D> {
    fn clone(&self) -> OutputPort<T, D> {
        OutputPort {
            limit:  self.limit,
            buffer: Vec::with_capacity(self.limit),
            spare:  Vec::with_capacity(self.limit),
            shared: self.shared.clone(),
        }
    }
}


// half of output_port used to add observers
pub struct Registrar<T, D> {
    shared: Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>>
}

impl<T: Timestamp, D: Data> Registrar<T, D> {
    pub fn add_observer<O: Observer<Time=T, Data=D>+'static>(&self, observer: O) {
        self.shared.borrow_mut().push(Box::new(observer));
    }
}

// TODO : Implemented on behalf of example_static::Stream; check if truly needed.
impl<T: Timestamp, D: Data> Clone for Registrar<T, D> {
    fn clone(&self) -> Registrar<T, D> { Registrar { shared: self.shared.clone() } }
}
