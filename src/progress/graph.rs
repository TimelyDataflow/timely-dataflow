use std::cell::RefCell;
use progress::{Timestamp, Scope, Subgraph};
use progress::nested::{Source, Target};
use communication::Communicator;

// cloneable so that we can make some copies, let different streams call connect.
pub trait Graph {
    type Timestamp : Timestamp;
    type Communicator : Communicator+Clone;

    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64;
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T>;
    fn communicator(&mut self) -> &mut Self::Communicator;

    fn builder(&mut self) -> RefCell<&mut Self>;
}

// // less terrifying wrapper for RefCell<&'b mut G>
// pub struct GraphBuilder<'b, G: Graph+'b> {
//     graph: RefCell<&'b mut G>
// }
//
// use core::cell::RefMut;
// impl<'b, G: Graph+'b> GraphBuilder<'b, G> {
//     pub fn graph<'a>(&'a mut self) -> RefMut<'a, &'b mut G> {
//         self.graph.borrow_mut()
//     }
// }

//
// impl<'b, G: Graph+'b> GraphBuilder<'b, G> {
//     pub fn connect(&mut self, source: Source, target: Target) { self.graph.borrow_mut().connect(source, target); }
//     pub fn add_scope<SC: Scope<G::Timestamp>+'static>(&mut self, scope: SC) -> u64 {
//         self.graph.borrow_mut().add_boxed_scope(Box::new(scope))
//     }
//     pub fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<G::Timestamp, T> { self.graph.borrow_mut().new_subgraph() }
//     pub fn communicator(&mut self) -> &mut G::Communicator { self.graph.borrow_mut().communicator() }
// }
