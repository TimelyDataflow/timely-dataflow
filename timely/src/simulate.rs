//! Deterministic simulation of multi-worker timely computations.
//!
//! A [`Simulation`] hosts several workers on the calling thread, using the ordinary
//! serializing intra-process allocator with each worker's receive [`Gate`] holding: every
//! message is physically received, but is logically delivered only when the caller says
//! so. Each worker is deterministic given the messages delivered to it, and delivery is
//! constrained only by per-source FIFO order, so an execution is a pure function of the
//! sequence of [`Decision`]s applied: the decision trace *is* the execution. Traces are
//! trivially recordable, replayable, and shrinkable, which makes this the substrate for
//! randomized schedule exploration ("deterministic simulation testing") of
//! progress-tracking behavior.
//!
//! # Examples
//! ```rust
//! use timely::simulate::{Decision, Simulation};
//! use timely::dataflow::operators::{ToStream, Inspect};
//!
//! let mut sim = Simulation::new(2);
//! for index in 0 .. 2 {
//!     sim.worker_mut(index).dataflow::<(),_,_>(|scope| {
//!         (0 .. 10).to_stream(scope)
//!                  .container::<Vec<_>>()
//!                  .inspect(|x| println!("seen: {:?}", x));
//!     });
//! }
//! // Interleave worker steps and message deliveries however the test likes ...
//! sim.apply(Decision::Step(0));
//! sim.apply(Decision::Deliver { source: 0, target: 1, count: 1 });
//! // ... then run to completion.
//! assert!(sim.drain(1_000));
//! ```

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;

use crate::WorkerConfig;
use crate::communication::Allocator;
use crate::communication::allocator::{Process, ProcessBuilder};
use crate::communication::allocator::zero_copy::gate::Gate;
use crate::communication::allocator::zero_copy::bytes_slab::BytesRefill;
use crate::worker::Worker;

/// One decision in a simulation schedule.
///
/// Any sequence of decisions is valid: stepping a worker with nothing to do and
/// delivering on an empty stream are both no-ops. This keeps randomly generated
/// and mechanically shrunk schedules well-formed by construction.
#[derive(Clone, Debug)]
pub enum Decision {
    /// Run one step of the indicated worker.
    Step(usize),
    /// Deliver up to `count` undelivered messages from `source` to `target`, in FIFO order.
    Deliver {
        /// The sending worker.
        source: usize,
        /// The receiving worker.
        target: usize,
        /// The maximum number of messages to deliver.
        count: usize,
    },
}

/// Several workers on one thread, with caller-controlled message delivery.
pub struct Simulation {
    /// Each worker's receive gate, holding for the duration of the simulation.
    gates: Vec<Rc<RefCell<Gate>>>,
    workers: Vec<Worker>,
}

impl Simulation {
    /// Creates a simulation of `peers` workers with default worker configuration.
    ///
    /// The workers are constructed without a timer, so time-based reschedulings
    /// (`activate_after`) degrade to immediate activation and logging is disabled;
    /// nothing in a simulated execution reads the wall clock.
    pub fn new(peers: usize) -> Self {
        let refill = BytesRefill {
            logic: Arc::new(|size| Box::new(vec![0_u8; size]) as Box<dyn std::ops::DerefMut<Target=[u8]>+Send>),
            limit: None,
        };
        let mut gates = Vec::with_capacity(peers);
        let mut workers = Vec::with_capacity(peers);
        let builders = ProcessBuilder::new_bytes_vector(peers, refill, None)
            .into_iter()
            .map(|builder| builder.holding(true))
            .collect();
        for process in ProcessBuilder::build_all(builders) {
            let gate = match &process {
                Process::Bytes(allocator) => allocator.gate(),
                _ => unreachable!("new_bytes_vector produces Bytes allocators"),
            };
            gates.push(gate);
            workers.push(Worker::new(WorkerConfig::default(), Allocator::Process(process), None));
        }
        Simulation { gates, workers }
    }

    /// The number of simulated workers.
    pub fn peers(&self) -> usize { self.workers.len() }

    /// Mutable access to a worker, e.g. to install dataflows or inspect probes.
    pub fn worker_mut(&mut self, index: usize) -> &mut Worker {
        &mut self.workers[index]
    }

    /// The gate of `target`, with everything sent to it so far fetched and held.
    fn gate(&self, target: usize) -> std::cell::RefMut<'_, Gate> {
        let mut gate = self.gates[target].borrow_mut();
        gate.fetch();
        gate
    }

    /// The number of undelivered messages from `source` to `target`.
    pub fn pending(&self, source: usize, target: usize) -> usize {
        self.gate(target).held(source)
    }

    /// Applies one schedule decision.
    pub fn apply(&mut self, decision: Decision) {
        match decision {
            Decision::Step(index) => { self.workers[index].step(); }
            Decision::Deliver { source, target, count } => { self.deliver(source, target, count); }
        }
    }

    /// Runs one step of the indicated worker, returning whether dataflows remain.
    pub fn step_worker(&mut self, index: usize) -> bool {
        self.workers[index].step()
    }

    /// Delivers up to `count` messages from `source` to `target`; returns the number delivered.
    pub fn deliver(&mut self, source: usize, target: usize, count: usize) -> usize {
        self.gate(target).release(source, count)
    }

    /// Runs the simulation to completion under a fair schedule: repeatedly deliver
    /// everything and step every worker, until no dataflows and no messages remain.
    ///
    /// Returns `true` if the simulation quiesced within `bound` rounds. A `false`
    /// return after a generous bound indicates a genuine liveness problem, as the
    /// schedule from here on is maximally fair.
    pub fn drain(&mut self, bound: usize) -> bool {
        for _ in 0 .. bound {
            let mut active = false;
            for target in 0 .. self.peers() {
                active |= self.gate(target).release_all() > 0;
            }
            for worker in self.workers.iter_mut() {
                active |= worker.step();
            }
            if !active { return true; }
        }
        false
    }
}
