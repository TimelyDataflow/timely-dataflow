use allocator::{Thread, Process, Binary};
use super::{Allocate, Data, Push, Pull};

pub enum Generic {
    Thread(Thread),
    Process(Process),
    Binary(Binary),
}

impl Generic {
    pub fn index(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.index(),
            &Generic::Process(ref p) => p.index(),
            &Generic::Binary(ref b) => b.index(),
        }
    }
    pub fn peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.peers(),
            &Generic::Process(ref p) => p.peers(),
            &Generic::Binary(ref b) => b.peers(),
        }
    }
    pub fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>) {
        match self {
            &mut Generic::Thread(ref mut t) => t.allocate(),
            &mut Generic::Process(ref mut p) => p.allocate(),
            &mut Generic::Binary(ref mut b) => b.allocate(),
        }
    }
}

impl Allocate for Generic {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>) { self.allocate() }
}
