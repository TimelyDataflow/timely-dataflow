//! The root of each single-threaded worker.

use std::rc::Rc;
use std::cell::{RefCell, RefMut};
use std::any::Any;
use std::time::{Instant, Duration};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::communication::{Allocate, Data, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::{Schedule, Scheduler, Activations};
use crate::progress::timestamp::{Refines};
use crate::progress::SubgraphBuilder;
use crate::progress::operate::Operate;
use crate::dataflow::scopes::Child;
use crate::logging::TimelyLogger;

/// Methods provided by the root Worker.
///
/// These methods are often proxied by child scopes, and this trait provides access.
pub trait AsWorker : Scheduler {

    /// Index of the worker among its peers.
    fn index(&self) -> usize;
    /// Number of peer workers.
    fn peers(&self) -> usize;
    /// Allocates a new channel from a supplied identifier and address.
    ///
    /// The identifier is used to identify the underlying channel and route
    /// its data. It should be distinct from other identifiers passed used
    /// for allocation, but can otherwise be arbitrary.
    ///
    /// The address should specify a path to an operator that should be
    /// scheduled in response to the receipt of records on the channel.
    /// Most commonly, this would be the address of the *target* of the
    /// channel.
    fn allocate<T: Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>);
    /// Constructs a pipeline channel from the worker to itself.
    ///
    /// By default this method uses the native channel allocation mechanism, but the expectation is
    /// that this behavior will be overriden to be more efficient.
    fn pipeline<T: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>);

    /// Allocates a new worker-unique identifier.
    fn new_identifier(&mut self) -> usize;
    /// Provides access to named logging streams.
    fn log_register(&self) -> ::std::cell::RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>>;
    /// Provides access to the timely logging stream.
    fn logging(&self) -> Option<crate::logging::TimelyLogger> { self.log_register().get("timely") }
}

/// A `Worker` is the entry point to a timely dataflow computation. It wraps a `Allocate`,
/// and has a list of dataflows that it manages.
pub struct Worker<A: Allocate> {
    timer: Instant,
    paths: Rc<RefCell<HashMap<usize, Vec<usize>>>>,
    allocator: Rc<RefCell<A>>,
    identifiers: Rc<RefCell<usize>>,
    // dataflows: Rc<RefCell<Vec<Wrapper>>>,
    dataflows: Rc<RefCell<HashMap<usize, Wrapper>>>,
    dataflow_counter: Rc<RefCell<usize>>,
    logging: Rc<RefCell<crate::logging_core::Registry<crate::logging::WorkerIdentifier>>>,

    activations: Rc<RefCell<Activations>>,
    active_dataflows: Vec<usize>,

    // Temporary storage for channel identifiers during dataflow construction.
    // These are then associated with a dataflow once constructed.
    temp_channel_ids: Rc<RefCell<Vec<usize>>>,
}

impl<A: Allocate> AsWorker for Worker<A> {
    fn index(&self) -> usize { self.allocator.borrow().index() }
    fn peers(&self) -> usize { self.allocator.borrow().peers() }
    fn allocate<D: Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<Push<Message<D>>>>, Box<Pull<Message<D>>>) {
        if address.len() == 0 { panic!("Unacceptable address: Length zero"); }
        let mut paths = self.paths.borrow_mut();
        paths.insert(identifier, address.to_vec());
        self.temp_channel_ids.borrow_mut().push(identifier);
        self.allocator.borrow_mut().allocate(identifier)
    }
    fn pipeline<T: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>) {
        if address.len() == 0 { panic!("Unacceptable address: Length zero"); }
        let mut paths = self.paths.borrow_mut();
        paths.insert(identifier, address.to_vec());
        self.temp_channel_ids.borrow_mut().push(identifier);
        self.allocator.borrow_mut().pipeline(identifier)
    }

    fn new_identifier(&mut self) -> usize { self.new_identifier() }
    fn log_register(&self) -> RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>> {
        self.log_register()
    }
}

impl<A: Allocate> Scheduler for Worker<A> {
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.activations.clone()
    }
}

impl<A: Allocate> Worker<A> {
    /// Allocates a new `Worker` bound to a channel allocator.
    pub fn new(c: A) -> Worker<A> {
        let now = Instant::now();
        let index = c.index();
        Worker {
            timer: now.clone(),
            paths: Rc::new(RefCell::new(HashMap::new())),
            allocator: Rc::new(RefCell::new(c)),
            identifiers: Rc::new(RefCell::new(0)),
            dataflows: Rc::new(RefCell::new(HashMap::new())),
            dataflow_counter: Rc::new(RefCell::new(0)),
            logging: Rc::new(RefCell::new(crate::logging_core::Registry::new(now, index))),
            activations: Rc::new(RefCell::new(Activations::new())),
            active_dataflows: Vec::new(),
            temp_channel_ids: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Performs one step of the computation.
    ///
    /// A step gives each dataflow operator a chance to run, and is the
    /// main way to ensure that a computation proceeds.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use timely::dataflow::operators::{ToStream, Inspect};
    ///
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x));
    ///     });
    ///
    ///     worker.step();
    /// });
    /// ```
    pub fn step(&mut self) -> bool {
        self.step_or_park(Some(Duration::from_secs(0)))
    }

    /// Performs one step of the computation.
    ///
    /// A step gives each dataflow operator a chance to run, and is the
    /// main way to ensure that a computation proceeds. This method may
    /// park the thread until there is work to perform, with an optional
    /// timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use std::time::Duration;
    ///     use timely::dataflow::operators::{ToStream, Inspect};
    ///
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x));
    ///     });
    ///
    ///     worker.step_or_park(Some(Duration::from_secs(1)));
    /// });
    /// ```
    pub fn step_or_park(&mut self, duration: Option<Duration>) -> bool {

        {   // Process channel events. Activate responders.
            let mut allocator = self.allocator.borrow_mut();
            allocator.receive();
            let events = allocator.events().clone();
            let mut borrow = events.borrow_mut();
            let paths = self.paths.borrow();
            for (channel, _event) in borrow.drain(..) {
                // TODO: Pay more attent to `_event`.
                // Consider tracking whether a channel
                // in non-empty, and only activating
                // on the basis of non-empty channels.
                // TODO: This is a sloppy way to deal
                // with channels that may not be alloc'd.
                if let Some(path) = paths.get(&channel) {
                    self.activations
                        .borrow_mut()
                        .activate(&path[..]);
                }
            }
        }

        // Organize activations.
        self.activations
            .borrow_mut()
            .advance();

        if self.activations.borrow().is_empty() && !self.dataflows.borrow().is_empty() {
            self.allocator
                .borrow()
                .await_events(duration);
        }
        else {   // Schedule active dataflows.

            let active_dataflows = &mut self.active_dataflows;
            self.activations
                .borrow_mut()
                .for_extensions(&[], |index| active_dataflows.push(index));

            let mut dataflows = self.dataflows.borrow_mut();
            for index in active_dataflows.drain(..) {
                // Step dataflow if it exists, remove if not incomplete.
                if let Entry::Occupied(mut entry) = dataflows.entry(index) {
                    let incomplete = entry.get_mut().step();
                    if !incomplete {
                        let mut paths = self.paths.borrow_mut();
                        for channel in entry.get_mut().channel_ids.drain(..) {
                            paths.remove(&channel);
                        }
                        entry.remove_entry();
                    }
                }
            }
        }

        // Clean up, indicate if dataflows remain.
        self.logging.borrow_mut().flush();
        self.allocator.borrow_mut().release();
        !self.dataflows.borrow().is_empty()
    }

    /// Calls `self.step()` as long as `func` evaluates to true.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use timely::dataflow::operators::{ToStream, Inspect, Probe};
    ///
    ///     let probe =
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x))
    ///             .probe()
    ///     });
    ///
    ///     worker.step_while(|| probe.less_than(&0));
    /// });
    /// ```
    pub fn step_while<F: FnMut()->bool>(&mut self, mut func: F) {
        while func() { self.step(); }
    }

    /// The index of the worker out of its peers.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     let index = worker.index();
    ///     let peers = worker.peers();
    ///     let timer = worker.timer();
    ///
    ///     println!("{:?}\tWorker {} of {}", timer.elapsed(), index, peers);
    ///
    /// });
    /// ```
    pub fn index(&self) -> usize { self.allocator.borrow().index() }
    /// The total number of peer workers.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     let index = worker.index();
    ///     let peers = worker.peers();
    ///     let timer = worker.timer();
    ///
    ///     println!("{:?}\tWorker {} of {}", timer.elapsed(), index, peers);
    ///
    /// });
    /// ```
    pub fn peers(&self) -> usize { self.allocator.borrow().peers() }

    /// A timer started at the initiation of the timely computation.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     let index = worker.index();
    ///     let peers = worker.peers();
    ///     let timer = worker.timer();
    ///
    ///     println!("{:?}\tWorker {} of {}", timer.elapsed(), index, peers);
    ///
    /// });
    /// ```
    pub fn timer(&self) -> Instant { self.timer }

    /// Allocate a new worker-unique identifier.
    ///
    /// This method is public, though it is not expected to be widely used outside
    /// of the timely dataflow system.
    pub fn new_identifier(&mut self) -> usize {
        *self.identifiers.borrow_mut() += 1;
        *self.identifiers.borrow() - 1
    }

    /// Access to named loggers.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     worker.log_register()
    ///           .insert::<timely::logging::TimelyEvent,_>("timely", |time, data|
    ///               println!("{:?}\t{:?}", time, data)
    ///           );
    /// });
    /// ```
    pub fn log_register(&self) -> ::std::cell::RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>> {
        self.logging.borrow_mut()
    }

    /// Construct a new dataflow.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     // We must supply the timestamp type here, although
    ///     // it would generally be determined by type inference.
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///
    ///         // uses of `scope` to build dataflow
    ///
    ///     });
    /// });
    /// ```
    pub fn dataflow<T, R, F>(&mut self, func: F) -> R
    where
        T: Refines<()>,
        F: FnOnce(&mut Child<Self, T>)->R,
    {
        let logging = self.logging.borrow_mut().get("timely");
        self.dataflow_core("Dataflow", logging, Box::new(()), |_, child| func(child))
    }

    /// Construct a new dataflow with specific configurations.
    ///
    /// This method constructs a new dataflow, using a name, logger, and additional
    /// resources specified as argument. The name is cosmetic, the logger is used to
    /// handle events generated by the dataflow, and the additional resources are kept
    /// alive for as long as the dataflow is alive (use case: shared library bindings).
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     // We must supply the timestamp type here, although
    ///     // it would generally be determined by type inference.
    ///     worker.dataflow_core::<usize,_,_,_>(
    ///         "dataflow X",           // Dataflow name
    ///         None,                   // Optional logger
    ///         37,                     // Any resources
    ///         |resources, scope| {    // Closure
    ///
    ///             // uses of `resources`, `scope`to build dataflow
    ///
    ///         }
    ///     );
    /// });
    /// ```
    pub fn dataflow_core<T, R, F, V>(&mut self, name: &str, mut logging: Option<TimelyLogger>, mut resources: V, func: F) -> R
    where
        T: Refines<()>,
        F: FnOnce(&mut V, &mut Child<Self, T>)->R,
        V: Any+'static,
    {
        let addr = vec![];
        let dataflow_index = self.allocate_dataflow_index();
        let identifier = self.new_identifier();

        let subscope = SubgraphBuilder::new_from(dataflow_index, addr, logging.clone(), name);
        let subscope = RefCell::new(subscope);

        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: logging.clone(),
            };
            func(&mut resources, &mut builder)
        };

        let mut operator = subscope.into_inner().build(self);

        logging.as_mut().map(|l| l.log(crate::logging::OperatesEvent {
            id: identifier,
            addr: operator.path().to_vec(),
            name: operator.name().to_string(),
        }));

        logging.as_mut().map(|l| l.flush());

        operator.get_internal_summary();
        operator.set_external_summary();

        let mut temp_channel_ids = self.temp_channel_ids.borrow_mut();
        let channel_ids = temp_channel_ids.drain(..).collect::<Vec<_>>();

        let wrapper = Wrapper {
            logging,
            identifier,
            operate: Some(Box::new(operator)),
            resources: Some(Box::new(resources)),
            channel_ids,
        };
        self.dataflows.borrow_mut().insert(dataflow_index, wrapper);

        result

    }

    // Acquire a new distinct dataflow identifier.
    fn allocate_dataflow_index(&mut self) -> usize {
        *self.dataflow_counter.borrow_mut() += 1;
        *self.dataflow_counter.borrow() - 1
    }
}

use crate::communication::Message;

impl<A: Allocate> Clone for Worker<A> {
    fn clone(&self) -> Self {
        Worker {
            timer: self.timer,
            paths: self.paths.clone(),
            allocator: self.allocator.clone(),
            identifiers: self.identifiers.clone(),
            dataflows: self.dataflows.clone(),
            dataflow_counter: self.dataflow_counter.clone(),
            logging: self.logging.clone(),
            activations: self.activations.clone(),
            active_dataflows: Vec::new(),
            temp_channel_ids: self.temp_channel_ids.clone(),
        }
    }
}

struct Wrapper {
    logging: Option<TimelyLogger>,
    identifier: usize,
    operate: Option<Box<Schedule>>,
    resources: Option<Box<Any>>,
    channel_ids: Vec<usize>,
}

impl Wrapper {
    /// Steps the dataflow, indicates if it remains incomplete.
    ///
    /// If the dataflow is incomplete, this call will drop it and its resources,
    /// dropping the dataflow first and then the resources (so that, e.g., shared
    /// library bindings will outlive the dataflow).
    fn step(&mut self) -> bool {

        // Perhaps log information about the start of the schedule call.
        if let Some(l) = self.logging.as_mut() {
            l.log(crate::logging::ScheduleEvent::start(self.identifier));
        }

        let incomplete = self.operate.as_mut().map(|op| op.schedule()).unwrap_or(false);
        if !incomplete {
            self.operate = None;
            self.resources = None;
        }

        // Perhaps log information about the stop of the schedule call.
        if let Some(l) = self.logging.as_mut() {
            l.log(crate::logging::ScheduleEvent::stop(self.identifier));
        }

        incomplete
    }
}

impl Drop for Wrapper {
    fn drop(&mut self) {
        if let Some(l) = self.logging.as_mut() {
            l.log(crate::logging::ShutdownEvent { id: self.identifier });
        }
        // ensure drop order
        self.operate = None;
        self.resources = None;
    }
}
