//! A child dataflow scope, used to build nested dataflow scopes.

use std::cell::RefCell;

use progress::{Timestamp, Operate, SubgraphBuilder};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use timely_communication::{Allocate, Data};
use {Push, Pull};
use logging::Logger;

use super::{ScopeParent, Scope};

/// A `Child` wraps a `Subgraph` and a parent `G: Scope`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct Child<'a, G: ScopeParent, T: Timestamp> {
    /// The subgraph under assembly.
    pub subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>,
    /// A copy of the child's parent scope.
    pub parent:   G,
    /// TODO(andreal)
    pub logging:  Logger,
}

impl<'a, G: ScopeParent, T: Timestamp> Child<'a, G, T> {
    /// This worker's unique identifier.
    ///
    /// Ranges from `0` to `self.peers() - 1`.
    pub fn index(&self) -> usize { self.parent.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.parent.peers() }
}

impl<'a, G: ScopeParent, T: Timestamp> ScopeParent for Child<'a, G, T> {
    type Timestamp = Product<G::Timestamp, T>;

    fn new_identifier(&mut self) -> usize {
        self.parent.new_identifier()
    }
}

impl<'a, G: ScopeParent, T: Timestamp> Scope for Child<'a, G, T> {
    fn name(&self) -> String { self.subgraph.borrow().name.clone() }
    fn addr(&self) -> Vec<usize> { self.subgraph.borrow().path.clone() }
    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_operator_with_index<SC: Operate<Self::Timestamp>+'static>(&mut self, scope: SC, index: usize) {
        let identifier = self.new_identifier();
        self.subgraph.borrow_mut().add_child(Box::new(scope), index, identifier);
    }

    fn allocate_operator_index(&mut self) -> usize {
        self.subgraph.borrow_mut().allocate_child_id()
    }

    // fn add_operator<SC: Operate<Self::Timestamp>+'static>(&mut self, scope: SC) -> usize {
    //     let index = self.subgraph.borrow_mut().allocate_child_id();
    //     self.add_operator_with_index(scope, index);
    //     index
    // }

    #[inline]
    fn scoped<T2: Timestamp, R, F: FnOnce(&mut Child<Self, T2>) -> R>(&mut self, func: F) -> R {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let path = self.subgraph.borrow().path.clone();

        let subscope = RefCell::new(SubgraphBuilder::new_from(index, path, self.logging().clone()));
        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);

        self.add_operator_with_index(subscope, index);

        result
    }

    fn logging(&self) -> Logger {
        self.logging.clone()
    }
}

impl<'a, G: ScopeParent, T: Timestamp> Allocate for Child<'a, G, T> {
    fn index(&self) -> usize { self.parent.index() }
    fn peers(&self) -> usize { self.parent.peers() }
    fn allocate<D: Data>(&mut self) -> (Vec<Box<Push<D>>>, Box<Pull<D>>, Option<usize>) {
        self.parent.allocate()
    }
}

impl<'a, G: ScopeParent, T: Timestamp> Clone for Child<'a, G, T> {
    fn clone(&self) -> Self { Child { subgraph: self.subgraph, parent: self.parent.clone(), logging: self.logging.clone() }}
}
