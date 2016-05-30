use std::rc::Rc;
use std::cell::RefCell;

use progress::timestamp::RootTimestamp;
use progress::{Timestamp, Operate, Subgraph};
use progress::nested::{Source, Target};
use timely_communication::{Allocate, Data};
use {Push, Pull};

use super::Scope;

/// A `Root` is the entry point to a timely dataflow computation. It wraps a `Allocate`,
/// and has a list of child `Operate`s. The primary intended use of `Root` is through its
/// implementation of the `Scope` trait.
pub struct Root<A: Allocate> {
    allocator: Rc<RefCell<A>>,
    graph: Rc<RefCell<Vec<Box<Operate<RootTimestamp>>>>>,
    identifiers: Rc<RefCell<usize>>,
}

impl<A: Allocate> Root<A> {
    pub fn new(c: A) -> Root<A> {
        let mut result = Root {
            allocator: Rc::new(RefCell::new(c)),
            graph: Rc::new(RefCell::new(Vec::new())),
            identifiers: Rc::new(RefCell::new(0)),
        };

        // LOGGING
        if cfg!(feature = "logging") {
            ::logging::initialize(&mut result);
        }

        result
    }
    /// Performs one step of the computation.
    ///
    /// A step gives each dataflow operator a chance to run, and is the 
    /// main way to ensure that a computation procedes.
    pub fn step(&mut self) -> bool {

        if cfg!(feature = "logging") {
            ::logging::flush_logs();
        }

        let mut active = false;
        for scope in self.graph.borrow_mut().iter_mut() {
            let sub_active = scope.pull_internal_progress(&mut [], &mut [], &mut []);
            active = active || sub_active;
        }
        active
    }
    /// Calls `self.step()` as long as `func` evaluates to true.
    pub fn step_while<F: FnMut()->bool>(&mut self, mut func: F) {
        while func() { self.step(); }
    }

    /// The index of the worker out of its peers.
    pub fn index(&self) -> usize { self.allocator.borrow().index() }
    /// The total number of peer workers.
    pub fn peers(&self) -> usize { self.allocator.borrow().peers() }
}

impl<A: Allocate> Scope for Root<A> {
    type Timestamp = RootTimestamp;

    fn name(&self) -> String { format!("Worker[{}]", self.allocator.borrow().index()) }
    fn addr(&self) -> Vec<usize> { vec![self.allocator.borrow().index() ] }
    fn add_edge(&self, _source: Source, _target: Target) {
        panic!("Root::connect(): root doesn't maintain edges; who are you, how did you get here?")
    }

    fn add_operator<SC: Operate<Self::Timestamp>+'static>(&mut self, scope: SC) -> usize {
        let index = self.graph.borrow().len();
        self.add_operator_with_index(scope, index);
        index
    }

    fn add_operator_with_index<SC: Operate<RootTimestamp>+'static>(&mut self, mut scope: SC, index: usize) {
        assert_eq!(index, self.graph.borrow().len());

        scope.get_internal_summary();
        scope.set_external_summary(Vec::new(), &mut []);
        self.graph.borrow_mut().push(Box::new(scope));
    }
    fn new_identifier(&mut self) -> usize {
        *self.identifiers.borrow_mut() += 1;
        *self.identifiers.borrow() - 1
    }

    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<RootTimestamp, T>  {
        let addr = vec![self.allocator.borrow().index()];
        Subgraph::new_from(&mut (*self.allocator.borrow_mut()), self.graph.borrow().len(), addr)
    }
}

impl<A: Allocate> Allocate for Root<A> {
    fn index(&self) -> usize { self.allocator.borrow().index() }
    fn peers(&self) -> usize { self.allocator.borrow().peers() }
    fn allocate<D: Data>(&mut self) -> (Vec<Box<Push<D>>>, Box<Pull<D>>) {
        self.allocator.borrow_mut().allocate()
    }
}

impl<A: Allocate> Clone for Root<A> {
    fn clone(&self) -> Self {
        Root {
            allocator: self.allocator.clone(),
            graph: self.graph.clone(),
            identifiers: self.identifiers.clone(),
        }
    }
}
