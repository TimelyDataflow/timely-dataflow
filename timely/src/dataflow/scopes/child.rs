//! A child dataflow scope, used to build nested dataflow scopes.

use std::rc::Rc;
use std::cell::RefCell;

use crate::communication::{Exchangeable, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::Scheduler;
use crate::scheduling::activate::Activations;
use crate::progress::{Timestamp, Operate, SubgraphBuilder};
use crate::progress::{Source, Target};
use crate::progress::timestamp::Refines;
use crate::order::Product;
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;
use crate::worker::{AsWorker, Config};

use super::{ScopeParent, Scope};

/// Type alias for iterative child scope.
pub type Iterative<'a, G, T> = Child<'a, G, Product<<G as ScopeParent>::Timestamp, T>>;

/// A `Child` wraps a `Subgraph` and a parent `G: Scope`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct Child<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>
{
    /// The subgraph under assembly.
    pub subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>,
    /// A copy of the child's parent scope.
    pub parent:   G,
    /// The log writer for this scope.
    pub logging:  Option<Logger>,
    /// The progress log writer for this scope.
    pub progress_logging:  Option<ProgressLogger>,
}

impl<G, T> Child<'_, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>
{
    /// This worker's unique identifier.
    ///
    /// Ranges from `0` to `self.peers() - 1`.
    pub fn index(&self) -> usize { self.parent.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.parent.peers() }
}

impl<G, T> AsWorker for Child<'_, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>
{
    fn config(&self) -> &Config { self.parent.config() }
    fn index(&self) -> usize { self.parent.index() }
    fn peers(&self) -> usize { self.parent.peers() }
    fn allocate<D: Exchangeable>(&mut self, identifier: usize, address: Rc<[usize]>) -> (Vec<Box<dyn Push<D>>>, Box<dyn Pull<D>>) {
        self.parent.allocate(identifier, address)
    }
    fn pipeline<D: 'static>(&mut self, identifier: usize, address: Rc<[usize]>) -> (ThreadPusher<D>, ThreadPuller<D>) {
        self.parent.pipeline(identifier, address)
    }
    fn new_identifier(&mut self) -> usize {
        self.parent.new_identifier()
    }
    fn peek_identifier(&self) -> usize {
        self.parent.peek_identifier()
    }
    fn log_register(&self) -> ::std::cell::RefMut<crate::logging_core::Registry> {
        self.parent.log_register()
    }
}

impl<G, T> Scheduler for Child<'_, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.parent.activations()
    }
}

impl<G, T> ScopeParent for Child<'_, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>
{
    type Timestamp = T;
}

impl<G, T> Scope for Child<'_, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
{
    fn name(&self) -> String { self.subgraph.borrow().name.clone() }
    fn addr(&self) -> Rc<[usize]> { Rc::clone(&self.subgraph.borrow().path) }

    fn addr_for_child(&self, index: usize) -> Rc<[usize]> {
        let path = &self.subgraph.borrow().path[..];
        let mut addr = Vec::with_capacity(path.len() + 1);
        addr.extend_from_slice(path);
        addr.push(index);
        addr.into()
    }

    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_operator_with_indices(&mut self, operator: Box<dyn Operate<Self::Timestamp>>, local: usize, global: usize) {
        self.subgraph.borrow_mut().add_child(operator, local, global);
    }

    fn allocate_operator_index(&mut self) -> usize {
        self.subgraph.borrow_mut().allocate_child_id()
    }

    #[inline]
    fn scoped<T2, R, F>(&mut self, name: &str, func: F) -> R
    where
        T2: Timestamp+Refines<T>,
        F: FnOnce(&mut Child<Self, T2>) -> R,
    {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let path = self.addr_for_child(index);

        let subscope = RefCell::new(SubgraphBuilder::new_from(path, self.logging(), self.progress_logging.clone(), name));
        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
                progress_logging: self.progress_logging.clone(),
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);

        self.add_operator_with_index(Box::new(subscope), index);

        result
    }
}

impl<G, T> Clone for Child<'_, G, T>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>
{
    fn clone(&self) -> Self {
        Child {
            subgraph: self.subgraph,
            parent: self.parent.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone(),
        }
    }
}
