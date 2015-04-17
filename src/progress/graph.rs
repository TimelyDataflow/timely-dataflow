use std::cell::RefCell;
use core::marker::PhantomData;
use progress::{Timestamp, Scope, Subgraph};
use progress::nested::{Source, Target};
use communication::Communicator;

pub trait Graph {
    type Timestamp : Timestamp;
    type Communicator : Communicator;

    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64;
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T>;

    fn with_communicator<R, F: FnOnce(&mut Self::Communicator)->R>(&mut self, func: F) -> R;

    fn builder(&mut self) -> RefCell<&mut Self>;
}

pub struct Root<T: Timestamp, C: Communicator> {
    communicator:   C,
    phantom:        PhantomData<T>,
}

impl<T: Timestamp, C: Communicator> Root<T, C> {
    pub fn new(c: C) -> Root<T, C> {
        Root {
            communicator: c,
            phantom: PhantomData,
        }
    }
}

impl<T: Timestamp, C: Communicator> Graph for Root<T, C> {
    type Timestamp = T;
    type Communicator = C;

    fn connect(&mut self, source: Source, target: Target) { panic!("no") }
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64  { panic!("bad") }
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subgraph<T2: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T2>  {
        Subgraph::<Self::Timestamp, T2>::new_from(&mut self.communicator, 0)
    }

    fn with_communicator<R, F: FnOnce(&mut Self::Communicator)->R>(&mut self, mut func: F) -> R {
        func(&mut self.communicator)
    }

    fn builder(&mut self) -> RefCell<&mut Self>  { RefCell::new(self) }
}
