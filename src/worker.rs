//! The root of each single-threaded worker.

use std::rc::Rc;
use std::cell::RefCell;
use std::any::Any;
use std::time::Instant;

use progress::nested::Refines;
use progress::timestamp::RootTimestamp;
use progress::{Timestamp, Operate, SubgraphBuilder};
use communication::{Allocate, Data, Push, Pull};
use dataflow::scopes::Child;

/// A `Worker` is the entry point to a timely dataflow computation. It wraps a `Allocate`,
/// and has a list of dataflows that it manages.
pub struct Worker<A: Allocate> {
    timer: Instant,
    allocator: Rc<RefCell<A>>,
    identifiers: Rc<RefCell<usize>>,
    dataflows: Rc<RefCell<Vec<Wrapper>>>,
    dataflow_counter: Rc<RefCell<usize>>,
    logging: Rc<RefCell<::logging_core::Registry<::logging::WorkerIdentifier>>>,
}

/// Methods provided by the root Worker.
///
/// These methods are often proxied by child scopes, and this trait provides access.
pub trait AsWorker: Allocate {
    /// Allocates a new worker-unique identifier.
    fn new_identifier(&mut self) -> usize;
    /// Provides access to named logging streams.
    fn log_register(&self) -> ::std::cell::RefMut<::logging_core::Registry<::logging::WorkerIdentifier>>;
    /// Provides access to the timely logging stream.
    fn logging(&self) -> Option<::logging::TimelyLogger> { self.log_register().get("timely") }
}

impl<A: Allocate> AsWorker for Worker<A> {
    fn new_identifier(&mut self) -> usize { self.new_identifier() }
    fn log_register(&self) -> ::std::cell::RefMut<::logging_core::Registry<::logging::WorkerIdentifier>> {
        self.log_register()
    }
}

impl<A: Allocate> Worker<A> {
    /// Allocates a new `Worker` bound to a channel allocator.
    pub fn new(c: A) -> Worker<A> {
        let now = Instant::now();
        let index = c.index();
        Worker {
            timer: now.clone(),
            allocator: Rc::new(RefCell::new(c)),
            identifiers: Rc::new(RefCell::new(0)),
            dataflows: Rc::new(RefCell::new(Vec::new())),
            dataflow_counter: Rc::new(RefCell::new(0)),
            logging: Rc::new(RefCell::new(::logging_core::Registry::new(now, index))),
        }
    }

    /// Performs one step of the computation.
    ///
    /// A step gives each dataflow operator a chance to run, and is the
    /// main way to ensure that a computation proceeds.
    pub fn step(&mut self) -> bool {

        self.allocator.borrow_mut().pre_work();

        let mut active = false;
        for dataflow in self.dataflows.borrow_mut().iter_mut() {
            let sub_active = dataflow.step();
            active = active || sub_active;
        }

        // discard completed dataflows.
        self.dataflows.borrow_mut().retain(|dataflow| dataflow.active());

        // TODO(andreal) do we want to flush logs here?
        self.logging.borrow_mut().flush();

        self.allocator.borrow_mut().post_work();

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

    /// A timer started at the initiation of the timely computation.
    pub fn timer(&self) -> Instant { self.timer }

    /// Allocate a new worker-unique identifier.
    pub fn new_identifier(&mut self) -> usize {
        *self.identifiers.borrow_mut() += 1;
        *self.identifiers.borrow() - 1
    }

    /// Access to named loggers.
    pub fn log_register(&self) -> ::std::cell::RefMut<::logging_core::Registry<::logging::WorkerIdentifier>> {
        self.logging.borrow_mut()
    }

    /// Construct a new dataflow.
    pub fn dataflow<T: Timestamp, R, F:FnOnce(&mut Child<Self, T>)->R>(&mut self, func: F) -> R
    where
        T: Refines<RootTimestamp>
    {
        self.dataflow_using(Box::new(()), |_, child| func(child))
    }

    /// Construct a new dataflow binding resources that are released only after the dataflow is dropped.
    ///
    /// This method is designed to allow the dataflow builder to use certain resources that are then stashed
    /// with the dataflow until it has completed running. Once complete, the resources are dropped. The most
    /// common use of this method at present is with loading shared libraries, where the library is important
    /// for building the dataflow, and must be kept around until after the dataflow has completed operation.
    pub fn dataflow_using<T: Timestamp, R, F:FnOnce(&mut V, &mut Child<Self, T>)->R, V: Any+'static>(&mut self, mut resources: V, func: F) -> R
    where
        T: Refines<RootTimestamp>,
    {

        let addr = vec![self.allocator.borrow().index()];
        let dataflow_index = self.allocate_dataflow_index();

        let mut logging = self.logging.borrow_mut().get("timely");
        let subscope = SubgraphBuilder::new_from(dataflow_index, addr, logging.clone(), "Dataflow");
        let subscope = RefCell::new(subscope);

        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: logging.clone(),
            };
            func(&mut resources, &mut builder)
        };

        logging.as_mut().map(|l| l.flush());

        let mut operator = subscope.into_inner().build(self);

        operator.get_internal_summary();
        operator.set_external_summary(Vec::new(), &mut []);

        let wrapper = Wrapper {
            _index: dataflow_index,
            operate: Some(Box::new(operator)),
            resources: Some(Box::new(resources)),
        };
        self.dataflows.borrow_mut().push(wrapper);

        result

    }

    // sane way to get new dataflow identifiers; used to be self.dataflows.len(). =/
    fn allocate_dataflow_index(&mut self) -> usize {
        *self.dataflow_counter.borrow_mut() += 1;
        *self.dataflow_counter.borrow() - 1
    }
}

use communication::Message;

impl<A: Allocate> Allocate for Worker<A> {
    fn index(&self) -> usize { self.allocator.borrow().index() }
    fn peers(&self) -> usize { self.allocator.borrow().peers() }
    fn allocate<D: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<D>>>>, Box<Pull<Message<D>>>) {
        self.allocator.borrow_mut().allocate(identifier)
    }
}

impl<A: Allocate> Clone for Worker<A> {
    fn clone(&self) -> Self {
        Worker {
            timer: self.timer,
            allocator: self.allocator.clone(),
            identifiers: self.identifiers.clone(),
            dataflows: self.dataflows.clone(),
            dataflow_counter: self.dataflow_counter.clone(),
            logging: self.logging.clone(),
        }
    }
}

struct Wrapper {
    _index: usize,
    operate: Option<Box<Operate<RootTimestamp>>>,
    resources: Option<Box<Any>>,
}

impl Wrapper {
    fn step(&mut self) -> bool {
        let active = self.operate.as_mut().map(|op| op.pull_internal_progress(&mut [], &mut [], &mut [])).unwrap_or(false);
        if !active {
            self.operate = None;
            self.resources = None;
        }
        // TODO consider flushing logs here (possibly after an arbitrary timeout)
        active
    }
    fn active(&self) -> bool { self.operate.is_some() }
}

impl Drop for Wrapper {
    fn drop(&mut self) {
        // println!("dropping dataflow {:?}", self._index);
        // ensure drop order
        self.operate = None;
        self.resources = None;
    }
}
