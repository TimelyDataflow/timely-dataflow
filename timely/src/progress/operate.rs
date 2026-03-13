//! Methods which describe an operators topology, and the progress it makes.

use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::Schedule;
use crate::progress::{Timestamp, ChangeBatch, Antichain};

/// A dataflow operator that progress with a specific timestamp type.
///
/// This trait describes the methods necessary to present as a dataflow operator.
/// This trait is a "builder" for operators, in that it reveals the structure of the operator
/// and its requirements, but then (through `initialize`) consumes itself to produce a boxed
/// schedulable object. At the moment of initialization, the values of the other methods are
/// captured and frozen.
pub trait Operate<T: Timestamp> {

    /// Indicates if the operator is strictly local to this worker.
    ///
    /// A parent scope must understand whether the progress information returned by the worker
    /// reflects only this worker's progress, so that it knows whether to send and receive the
    /// corresponding progress messages to its peers. If the operator is strictly local, it must
    /// exchange this information, whereas if the operator is itself implemented by the same set
    /// of workers, the parent scope understands that progress information already reflects the
    /// aggregate information among the workers.
    ///
    /// This is a coarse approximation to refined worker sets. In a future better world, operators
    /// would explain how their implementations are partitioned, so that a parent scope knows what
    /// progress information to exchange with which peers. Right now the two choices are either
    /// "all" or "none", but it could be more detailed. In the more detailed case, this method
    /// should / could return a pair (index, peers), indicating the group id of the worker out of
    /// how many groups. This becomes complicated, as a full all-to-all exchange would result in
    /// multiple copies of the same progress messages (but aggregated variously) arriving at
    /// arbitrary times.
    fn local(&self) -> bool { true }

    /// The number of inputs.
    fn inputs(&self) -> usize;
    /// The number of outputs.
    fn outputs(&self) -> usize;

    /// Initializes the operator, converting the operator builder to a schedulable object.
    ///
    /// In addition, initialization produces internal connectivity, and a shared progress conduit
    /// which must contain any initial output capabilities the operator would like to hold.
    ///
    /// The internal connectivity summarizes the operator by a map from pairs `(input, output)`
    /// to an antichain of timestamp summaries, indicating how a timestamp on any of its inputs may
    /// be transformed to timestamps on any of its outputs. The conservative and most common result
    /// is full connectivity between all inputs and outputs, each with the identity summary.
    ///
    /// The shared progress object allows information to move between the host and the schedulable.
    /// Importantly, it also indicates the initial internal capabilities for all of its outputs.
    /// This must happen at this moment, as it is the only moment where an operator is allowed to
    /// safely "create" capabilities without basing them on other, prior capabilities.
    fn initialize(self: Box<Self>) -> (Connectivity<T::Summary>, Rc<RefCell<SharedProgress<T>>>, Box<dyn Schedule>);

    /// Indicates for each input whether the operator should be invoked when that input's frontier changes.
    ///
    /// Returns a `Vec<FrontierInterest>` with one entry per input. Each entry describes whether
    /// frontier changes on that input should cause the operator to be scheduled. The conservative
    /// default is `Always` for each input.
    fn notify_me(&self) -> &[FrontierInterest];// { &vec![FrontierInterest::Always; self.inputs()] }

    /// Indicates whether all inputs use thread-local (pipeline) channels.
    ///
    /// Operators with pipeline inputs receive data through thread-local channels,
    /// meaning data pushed by an upstream operator on the same worker is immediately
    /// available. This property is used by chain fusion to determine whether
    /// consecutive operators can be scheduled as a single unit.
    fn pipeline(&self) -> bool { true }
}

/// The ways in which an operator can express interest in activation when an input frontier changes.
#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub enum FrontierInterest {
    /// Never interested in frontier changes, as for example the `map()` and `filter()` operators.
    Never,
    /// Interested when the operator holds capabilities.
    IfCapability,
    /// Always interested in frontier changes, as for example the `probe()` and `capture()` operators.
    Always,
}

/// Operator internal connectivity, from inputs to outputs.
pub type Connectivity<TS> = Vec<PortConnectivity<TS>>;
/// Internal connectivity from one port to any number of opposing ports.
#[derive(serde::Serialize, serde::Deserialize, columnar::Columnar, Debug, Clone, Eq, PartialEq)]
pub struct PortConnectivity<TS> {
    tree: std::collections::BTreeMap<usize, Antichain<TS>>,
}

impl<TS> Default for PortConnectivity<TS> {
    fn default() -> Self {
        Self { tree: std::collections::BTreeMap::new() }
    }
}

impl<TS> PortConnectivity<TS> {
    /// Inserts an element by reference, ensuring that the index exists.
    pub fn insert(&mut self, index: usize, element: TS) -> bool where TS : crate::PartialOrder {
        self.tree.entry(index).or_default().insert(element)
    }
    /// Inserts an element by reference, ensuring that the index exists.
    pub fn insert_ref(&mut self, index: usize, element: &TS) -> bool where TS : crate::PartialOrder + Clone {
        self.tree.entry(index).or_default().insert_ref(element)
    }
    /// Introduces a summary for `port`. Panics if a summary already exists.
    pub fn add_port(&mut self, port: usize, summary: Antichain<TS>) {
        if !summary.is_empty() {
            let prior = self.tree.insert(port, summary);
            assert!(prior.is_none());
        }
        else {
            assert!(self.tree.remove(&port).is_none());
        }
    }
    /// Borrowing iterator of port identifiers and antichains.
    pub fn iter_ports(&self) -> impl Iterator<Item = (usize, &Antichain<TS>)> {
        self.tree.iter().map(|(o,p)| (*o, p))
    }
    /// Returns the associated path summary, if it exists.
    pub fn get(&self, index: usize) -> Option<&Antichain<TS>> {
        self.tree.get(&index)
    }
}

impl<TS> FromIterator<(usize, Antichain<TS>)> for PortConnectivity<TS> {
    fn from_iter<T>(iter: T) -> Self where T: IntoIterator<Item = (usize, Antichain<TS>)> {
        Self { tree: iter.into_iter().filter(|(_,p)| !p.is_empty()).collect() }
    }
}

/// Progress information shared between parent and child.
#[derive(Debug)]
pub struct SharedProgress<T: Timestamp> {
    /// Frontier capability changes reported by the parent scope.
    pub frontiers: Vec<ChangeBatch<T>>,
    /// Consumed message changes reported by the child operator.
    pub consumeds: Vec<ChangeBatch<T>>,
    /// Internal capability changes reported by the child operator.
    pub internals: Vec<ChangeBatch<T>>,
    /// Produced message changes reported by the child operator.
    pub produceds: Vec<ChangeBatch<T>>,
}

impl<T: Timestamp> SharedProgress<T> {
    /// Allocates a new shared progress structure.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        SharedProgress {
            frontiers: vec![ChangeBatch::new(); inputs],
            consumeds: vec![ChangeBatch::new(); inputs],
            internals: vec![ChangeBatch::new(); outputs],
            produceds: vec![ChangeBatch::new(); outputs],
        }
    }
}
