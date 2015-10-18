use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Operate, Subgraph};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
// use progress::nested::scope_wrapper::ChildWrapper;
use timely_communication::{Allocate, Data};
use {Push, Pull};

use super::Scope;

/// A `Child` wraps a `Subgraph` and a parent `G: Scope`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct Child<G: Scope, T: Timestamp> {
    pub subgraph: Rc<RefCell<Subgraph<G::Timestamp, T>>>,
    pub parent:   G,
}

impl<G: Scope, T: Timestamp> Scope for Child<G, T> {
    type Timestamp = Product<G::Timestamp, T>;

    fn name(&self) -> String { self.subgraph.borrow().name().to_owned() }
    fn addr(&self) -> Vec<usize> { self.subgraph.borrow().path.clone() }
    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_operator_with_index<SC: Operate<Self::Timestamp>+'static>(&self, scope: SC, index: usize) {
        self.subgraph.borrow_mut().add_child(Box::new(scope), index);
    }

    fn add_operator<SC: Operate<Self::Timestamp>+'static>(&self, scope: SC) -> usize {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        self.add_operator_with_index(scope, index);
        index
    }
    
    fn new_identifier(&mut self) -> usize {
        self.parent.new_identifier()
    }

    fn new_subscope<T2: Timestamp>(&mut self) -> Subgraph<Product<G::Timestamp, T>, T2> {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let path = self.subgraph.borrow().path.clone();
        Subgraph::new_from(self, index, path)
    }
}

impl<G: Scope, T: Timestamp> Allocate for Child<G, T> {
    fn index(&self) -> usize { self.parent.index() }
    fn peers(&self) -> usize { self.parent.peers() }
    fn allocate<D: Data>(&mut self) -> (Vec<Box<Push<D>>>, Box<Pull<D>>) {
        self.parent.allocate()
    }
}

impl<G: Scope, T: Timestamp> Clone for Child<G, T> {
    fn clone(&self) -> Self { Child { subgraph: self.subgraph.clone(), parent: self.parent.clone() }}
}
