//! Types to build operators with general shapes.
//!
//! These types expose some raw timely interfaces, and while public so that others can build on them,
//! they require some sophistication to use correctly. I recommend checking out `builder_rc.rs` for
//! an interface that is intentionally harder to mis-use.

use std::default::Default;
use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::{Schedule, Activations};
use crate::worker::AsWorker;
use crate::scheduling::Scheduler;

use crate::progress::{Source, Target};
use crate::progress::{Timestamp, Operate, operate::SharedProgress, Antichain};
use crate::progress::operate::{FrontierInterest, Connectivity, PortConnectivity};
use crate::Container;
use crate::dataflow::{Stream, Scope, OperatorSlot};
use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::ParallelizationContract;
use crate::dataflow::operators::generic::operator_info::OperatorInfo;

/// Contains type-free information about the operator properties.
#[derive(Debug)]
pub struct OperatorShape {
    name: String,   // A meaningful name for the operator.
    notify: Vec<FrontierInterest>,   // Per-input frontier interest.
    peers: usize,   // The total number of workers in the computation. Needed to initialize pointstamp counts with the correct magnitude.
    inputs: usize,  // The number of input ports.
    outputs: usize, // The number of output ports.
}

/// Core data for the structure of an operator, minus scope and logic.
impl OperatorShape {
    fn new(name: String, peers: usize) -> Self {
        OperatorShape {
            name,
            notify: Vec::new(),
            peers,
            inputs: 0,
            outputs: 0,
        }
    }

    /// The number of inputs of this operator
    pub fn inputs(&self) -> usize { self.inputs }

    /// The number of outputs of this operator
    pub fn outputs(&self) -> usize { self.outputs }
}

/// Builds operators with generic shape.
#[derive(Debug)]
pub struct OperatorBuilder<'scope, T: Timestamp> {
    scope: Scope<'scope, T>,
    slot: OperatorSlot<'scope, T>,
    address: Rc<[usize]>,    // path to the operator (ending with index).
    shape: OperatorShape,
    summary: Connectivity<<T as Timestamp>::Summary>,
}

impl<'scope, T: Timestamp> OperatorBuilder<'scope, T> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, mut scope: Scope<'scope, T>) -> Self {

        let slot = scope.reserve_operator();
        let address = slot.addr();
        let peers = scope.peers();

        OperatorBuilder {
            scope,
            slot,
            address,
            shape: OperatorShape::new(name, peers),
            summary: vec![],
        }
    }

    /// The operator's scope-local index.
    pub fn index(&self) -> usize { self.slot.index() }

    /// The operator's worker-unique identifier.
    pub fn global(&self) -> usize { self.slot.identifier() }

    /// Return a reference to the operator's shape
    pub fn shape(&self) -> &OperatorShape { &self.shape }

    /// Sets frontier interest for a specific input.
    pub fn set_notify_for(&mut self, input: usize, notify: FrontierInterest) {
        self.shape.notify[input] = notify;
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<C: Container, P>(&mut self, stream: Stream<T, C>, pact: P) -> P::Puller
    where
        P: ParallelizationContract<T, C>
    {
        let connection = (0 .. self.shape.outputs).map(|o| (o, Antichain::from_elem(Default::default())));
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input_connection<C: Container, P, I>(&mut self, stream: Stream<T, C>, pact: P, connection: I) -> P::Puller
    where
        P: ParallelizationContract<T, C>,
        I: IntoIterator<Item = (usize, Antichain<<T as Timestamp>::Summary>)>,
    {
        let channel_id = self.scope.new_identifier();
        let logging = self.scope.logging();
        let (sender, receiver) = pact.connect(&mut self.scope, channel_id, Rc::clone(&self.address), logging);
        let target = Target::new(self.slot.index(), self.shape.inputs);
        stream.connect_to(target, sender, channel_id);

        self.shape.inputs += 1;
        self.shape.notify.push(FrontierInterest::Always);
        let connectivity: PortConnectivity<_> = connection.into_iter().collect();
        assert!(connectivity.iter_ports().all(|(o,_)| o < self.shape.outputs));
        self.summary.push(connectivity);

        receiver
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<C: Container>(&mut self) -> (Tee<T, C>, Stream<'scope, T, C>) {
        let connection = (0 .. self.shape.inputs).map(|i| (i, Antichain::from_elem(Default::default())));
        self.new_output_connection(connection)
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output_connection<C: Container, I>(&mut self, connection: I) -> (Tee<T, C>, Stream<'scope, T, C>)
    where
        I: IntoIterator<Item = (usize, Antichain<<T as Timestamp>::Summary>)>,
    {
        let new_output = self.shape.outputs;
        self.shape.outputs += 1;
        let (target, registrar) = Tee::new();
        let source = Source::new(self.slot.index(), new_output);
        let stream = Stream::new(source, registrar, self.scope.clone());

        for (input, entry) in connection {
            self.summary[input].add_port(new_output, entry);
        }

        (target, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<L>(self, logic: L)
    where
        L: FnMut(&mut SharedProgress<T>)->bool+'static
    {
        let inputs = self.shape.inputs;
        let outputs = self.shape.outputs;

        let operator = OperatorCore {
            shape: self.shape,
            address: self.address,
            activations: self.scope.activations(),
            logic,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),
            summary: self.summary,
        };

        self.slot.install(Box::new(operator));
    }

    /// Information describing the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        OperatorInfo::new(self.index(), self.global(), Rc::clone(&self.address))
    }
}

struct OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>)->bool+'static,
{
    shape: OperatorShape,
    address: Rc<[usize]>,
    logic: L,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    activations: Rc<RefCell<Activations>>,
    summary: Connectivity<T::Summary>,
}

impl<T, L> Schedule for OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>)->bool+'static,
{
    fn name(&self) -> &str { &self.shape.name }
    fn path(&self) -> &[usize] { &self.address[..] }
    fn schedule(&mut self) -> bool {
        let shared_progress = &mut *self.shared_progress.borrow_mut();
        (self.logic)(shared_progress)
    }
}

impl<T, L> Operate<T> for OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>)->bool+'static,
{
    fn inputs(&self) -> usize { self.shape.inputs }
    fn outputs(&self) -> usize { self.shape.outputs }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn initialize(self: Box<Self>) -> (Connectivity<T::Summary>, Rc<RefCell<SharedProgress<T>>>, Box<dyn Schedule>) {

        // Request the operator to be scheduled at least once.
        self.activations.borrow_mut().activate(&self.address[..]);

        // by default, we reserve a capability for each output port at `Default::default()`.
        self.shared_progress
            .borrow_mut()
            .internals
            .iter_mut()
            .for_each(|output| output.update(T::minimum(), self.shape.peers as i64));

        (self.summary.clone(), Rc::clone(&self.shared_progress), self)
    }

    fn notify_me(&self) -> &[FrontierInterest] { &self.shape.notify }
}
