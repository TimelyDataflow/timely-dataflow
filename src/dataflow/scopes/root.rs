//! The root scope of all timely dataflow computations.

use std::rc::Rc;
use std::cell::RefCell;

use progress::timestamp::RootTimestamp;
use progress::{Timestamp, Operate, Subgraph};
use timely_communication::{Allocate, Data};
use {Push, Pull};

use super::{ScopeParent, Child};

/// A `Root` is the entry point to a timely dataflow computation. It wraps a `Allocate`,
/// and has a list of child `Operate`s.
pub struct Root<A: Allocate> {
    allocator: Rc<RefCell<A>>,
    identifiers: Rc<RefCell<usize>>,
    dataflows: Rc<RefCell<Vec<Wrapper>>>,
    dataflow_counter: Rc<RefCell<usize>>,
}

impl<A: Allocate> Root<A> {
    /// Allocates a new `Root` bound to a channel allocator.
    pub fn new(c: A) -> Root<A> {
        let mut result = Root {
            allocator: Rc::new(RefCell::new(c)),
            identifiers: Rc::new(RefCell::new(0)),
            dataflows: Rc::new(RefCell::new(Vec::new())),
            dataflow_counter: Rc::new(RefCell::new(0)),
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
        for dataflow in self.dataflows.borrow_mut().iter_mut() {
            let sub_active = dataflow.step();
            active = active || sub_active;
        }

        // discard completed dataflows.
        self.dataflows.borrow_mut().retain(|dataflow| dataflow.active());

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

    /// Introduces a new dataflow constructed in the supplied closure.
    ///
    /// NOTE: This is the same code as in the `scoped` method for scopes, but it has been extracted
    /// so that the `Root` type does not need to implement `Scope`.
    pub fn dataflow<T: Timestamp, R, F:FnOnce(&mut Child<Self, T>)->R>(&mut self, func: F) -> R {

        let addr = vec![self.allocator.borrow().index()];
        let dataflow_index = self.allocate_dataflow_index();
        let subscope = Subgraph::new_from(&mut (*self.allocator.borrow_mut()), dataflow_index, addr);
        let subscope = RefCell::new(subscope);

        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
            };
            func(&mut builder)
        };

        let mut operator = subscope.into_inner();

        operator.get_internal_summary();
        operator.set_external_summary(Vec::new(), &mut []);

        let wrapper = Wrapper {
            index: dataflow_index,
            operate: Some(Box::new(operator)),
            resources: None,
        };
        self.dataflows.borrow_mut().push(wrapper);

        result
    }

    /// Manually construct a dataflow.
    pub fn construct_dataflow<O: Operate<RootTimestamp>+'static, F: FnOnce(usize)->O>(&mut self, func: F) {
        let index = self.allocate_dataflow_index();
        let mut operator = func(index);

        // prepare progress tracking for operator.
        operator.get_internal_summary();
        operator.set_external_summary(Vec::new(), &mut []);

        let wrapper = Wrapper {
            index: index,
            operate: Some(Box::new(operator)),
            resources: None,
        };
        self.dataflows.borrow_mut().push(wrapper);
    }

    // sane way to get new dataflow identifiers; used to be self.dataflows.len(). =/
    fn allocate_dataflow_index(&mut self) -> usize {
        *self.dataflow_counter.borrow_mut() += 1;
        *self.dataflow_counter.borrow() - 1
    }
}

impl<A: Allocate> ScopeParent for Root<A> {
    type Timestamp = RootTimestamp;

    fn new_identifier(&mut self) -> usize {
        *self.identifiers.borrow_mut() += 1;
        *self.identifiers.borrow() - 1
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
            identifiers: self.identifiers.clone(),
            dataflows: self.dataflows.clone(),
            dataflow_counter: self.dataflow_counter.clone(),
        }
    }
}

struct Wrapper {
    index: usize,
    operate: Option<Box<Operate<RootTimestamp>>>,
    resources: Option<Box<Drop>>,
}

impl Wrapper {
    fn step(&mut self) -> bool {
        self.operate.as_mut().map(|op| op.pull_internal_progress(&mut [], &mut [], &mut [])).unwrap_or(false)
    }
    fn active(&self) -> bool { self.operate.is_some() }
}

impl Drop for Wrapper {
    fn drop(&mut self) {
        println!("dropping dataflow {:?}", self.index);
        // ensure drop order
        self.operate = None;
        self.resources = None;
    }
}
