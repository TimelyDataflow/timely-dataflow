//! A child dataflow scope, used to build nested dataflow scopes.

use std::rc::Rc;
use std::cell::RefCell;

use crate::communication::{Allocate, Exchangeable, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::Scheduler;
use crate::scheduling::activate::Activations;
use crate::progress::{Timestamp, Operate, SubgraphBuilder};
use crate::progress::{Source, Target};
use crate::progress::timestamp::Refines;
use crate::order::Product;
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;
use crate::worker::{AsWorker, Config, Worker};

use super::{ScopeParent, Scope};

/// Type alias for iterative child scope.
pub type Iterative<'a, A, TOuter, TInner> = Child<'a, A, Product<TOuter, TInner>>;

/// A `Child` wraps a `Subgraph` and manages the addition
/// of `Operate`s and the connection of edges between them.
pub struct Child<'a, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    /// The subgraph under assembly.
    pub subgraph: &'a RefCell<SubgraphBuilder<T>>,
    /// A copy of the worker hosting this scope.
    pub worker:   Worker<A>,
    /// The log writer for this scope.
    pub logging:  Option<Logger>,
    /// The progress log writer for this scope.
    pub progress_logging:  Option<ProgressLogger<T>>,
}

impl<A, T> Child<'_, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    /// This worker's unique identifier.
    ///
    /// Ranges from `0` to `self.peers() - 1`.
    pub fn index(&self) -> usize { self.worker.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.worker.peers() }
}

impl<A, T> AsWorker for Child<'_, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    fn config(&self) -> &Config { self.worker.config() }
    fn index(&self) -> usize { self.worker.index() }
    fn peers(&self) -> usize { self.worker.peers() }
    fn allocate<D: Exchangeable>(&mut self, identifier: usize, address: Rc<[usize]>) -> (Vec<Box<dyn Push<D>>>, Box<dyn Pull<D>>) {
        self.worker.allocate(identifier, address)
    }
    fn pipeline<D: 'static>(&mut self, identifier: usize, address: Rc<[usize]>) -> (ThreadPusher<D>, ThreadPuller<D>) {
        self.worker.pipeline(identifier, address)
    }
    fn broadcast<D: Exchangeable + Clone>(&mut self, identifier: usize, address: Rc<[usize]>) -> (Box<dyn Push<D>>, Box<dyn Pull<D>>) {
        self.worker.broadcast(identifier, address)
    }
    fn new_identifier(&mut self) -> usize {
        self.worker.new_identifier()
    }
    fn peek_identifier(&self) -> usize {
        self.worker.peek_identifier()
    }
    fn log_register(&self) -> Option<::std::cell::RefMut<'_, crate::logging_core::Registry>> {
        self.worker.log_register()
    }
}

impl<A, T> Scheduler for Child<'_, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.worker.activations()
    }
}

impl<A, T> ScopeParent for Child<'_, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    type Timestamp = T;
}

impl<A, T> Scope for Child<'_, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    type Allocator = A;
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
    fn scoped<T2, R, F>(&self, name: &str, func: F) -> R
    where
        T2: Timestamp+Refines<T>,
        F: FnOnce(&mut Child<A, T2>) -> R,
    {
        let mut scope = self.clone();
        let index = scope.subgraph.borrow_mut().allocate_child_id();
        let identifier = scope.new_identifier();
        let path = scope.addr_for_child(index);

        let type_name = std::any::type_name::<T2>();
        let progress_logging = scope.logger_for(&format!("timely/progress/{type_name}"));
        let summary_logging  = scope.logger_for(&format!("timely/summary/{type_name}"));

        let subscope = RefCell::new(SubgraphBuilder::new_from(path, identifier, self.logging(), summary_logging, name));
        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                worker: scope.worker.clone(),
                logging: scope.logging.clone(),
                progress_logging,
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(&mut scope);

        scope.add_operator_with_indices(Box::new(subscope), index, identifier);

        result
    }
}

impl<A, T> Clone for Child<'_, A, T>
where
    A: Allocate,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Child {
            subgraph: self.subgraph,
            worker: self.worker.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone(),
        }
    }
}
