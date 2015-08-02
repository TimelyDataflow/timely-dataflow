use communication::communicator::{Thread, Process, Binary};
use communication::{Pullable, Communicator, Data};
use communication::observer::BoxedObserver;
use serialization::Serializable;

pub enum Generic {
    Thread(Thread),
    Process(Process),
    Binary(Binary),
}

impl Communicator for Generic {
    fn index(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.index(),
            &Generic::Process(ref p) => p.index(),
            &Generic::Binary(ref b) => b.index(),
        }
    }
    fn peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.peers(),
            &Generic::Process(ref p) => p.peers(),
            &Generic::Binary(ref b) => b.peers(),
        }
    }
    fn new_channel<T: Data+Serializable, D: Data+Serializable>(&mut self) -> (Vec<BoxedObserver<T, D>>, Box<Pullable<T, D>>) {
        match self {
            &mut Generic::Thread(ref mut t) => t.new_channel(),
            &mut Generic::Process(ref mut p) => p.new_channel(),
            &mut Generic::Binary(ref mut b) => b.new_channel(),
        }
    }
}
