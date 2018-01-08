//! Types to build operators with general shapes.
//!
//! These types expose some raw timely interfaces, and while public so that others can build on them,
//! they require some sophistication to use correctly. I recommend checking out `builder_rc.rs` for 
//! an interface that is intentionally harder to mis-use.

use std::default::Default;

use ::Data;

use progress::nested::subgraph::{Source, Target};
use progress::ChangeBatch;
use progress::{Timestamp, Operate, Antichain};

use dataflow::{Stream, Scope};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pact::ParallelizationContract;

/// Contains type-free information about the operator properties.
pub struct OperatorShape {
    name: String,   // A meaningful name for the operator.
    notify: bool,   // Does the operator require progress notifications.
    peers: usize,   // The total number of workers in the computation.
    inputs: usize,  // The number of input ports.
    outputs: usize, // The number of output ports.
}

/// Core data for the structure of an operator, minus scope and logic.
impl OperatorShape {
    fn new(name: String, peers: usize) -> Self {
        OperatorShape {
            name: name,
            notify: true,
            peers: peers,
            inputs: 0,
            outputs: 0,
        }
    }

    /// The number of inputs of this operator
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// The number of outputs of this operator
    pub fn outputs(&self) -> usize {
        self.outputs
    }
}

/// Builds operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    scope: G,
    index: usize,
    shape: OperatorShape,
    summary: Vec<Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>>,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, mut scope: G) -> Self {

        let index = scope.allocate_operator_index();
        let peers = scope.peers();

        OperatorBuilder {
            scope: scope,
            index: index,
            shape: OperatorShape::new(name, peers),
            summary: vec![],
        }
    }

    /// The operator's index
    pub fn index(&self) -> usize {
        self.index
    }

    /// Return a reference to the operator's shape
    pub fn shape(&self) -> &OperatorShape {
        &self.shape
    }

    /// Indicates whether the operator requires frontier information.
    pub fn set_notify(&mut self, notify: bool) {
        self.shape.notify = notify;
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P) -> P::Puller
        where
            P: ParallelizationContract<G::Timestamp, D> {
        let connection = vec![Antichain::from_elem(Default::default()); self.shape.outputs];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input_connection<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> P::Puller
    where
        P: ParallelizationContract<G::Timestamp, D> {

        let channel_id = self.scope.new_identifier();
        let logging = self.scope.logging();
        let (sender, receiver) = pact.connect(&mut self.scope, channel_id, logging);
        let target = Target { index: self.index, port: self.shape.inputs };
        stream.connect_to(target, sender, channel_id);
        
        self.shape.inputs += 1;
        assert_eq!(self.shape.outputs, connection.len());
        self.summary.push(connection);

        receiver
    }

    /// Adds a new input to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (Tee<G::Timestamp, D>, Stream<G, D>) {

        let connection = vec![Antichain::from_elem(Default::default()); self.shape.inputs];
        self.new_output_connection(connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output_connection<D: Data>(&mut self, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> (Tee<G::Timestamp, D>, Stream<G, D>) {

        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let source = Source { index: self.index, port: self.shape.outputs };
        let stream = Stream::new(source, registrar, self.scope.clone());

        self.shape.outputs += 1;
        assert_eq!(self.shape.inputs, connection.len());
        for (summary, entry) in self.summary.iter_mut().zip(connection.into_iter()) {
            summary.push(entry);
        }

        (targets, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<PEP, PIP>(mut self, push_external: PEP, pull_internal: PIP) 
    where 
        PEP: FnMut(&mut [ChangeBatch<G::Timestamp>])+'static,
        PIP: FnMut(
            &mut [ChangeBatch<G::Timestamp>],
            &mut [ChangeBatch<G::Timestamp>],
            &mut [ChangeBatch<G::Timestamp>],
        )->bool+'static
    {
        let operator = OperatorCore {
            shape: self.shape,
            push_external: push_external,
            pull_internal: pull_internal,
            summary: self.summary,
            phantom: ::std::marker::PhantomData,
        };

        self.scope.add_operator_with_index(operator, self.index);
    }
}

struct OperatorCore<T, PEP, PIP> 
    where 
        T: Timestamp,
        PEP: FnMut(&mut [ChangeBatch<T>])+'static,
        PIP: FnMut(&mut [ChangeBatch<T>], &mut [ChangeBatch<T>], &mut [ChangeBatch<T>])->bool+'static
{
    shape: OperatorShape,
    push_external: PEP,
    pull_internal: PIP,
    summary: Vec<Vec<Antichain<T::Summary>>>,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, PEP, PIP> Operate<T> for OperatorCore<T, PEP, PIP> 
    where 
        T: Timestamp,
        PEP: FnMut(&mut [ChangeBatch<T>])+'static,
        PIP: FnMut(&mut [ChangeBatch<T>], &mut [ChangeBatch<T>], &mut [ChangeBatch<T>])->bool+'static
{
    fn inputs(&self) -> usize { self.shape.inputs }
    fn outputs(&self) -> usize { self.shape.outputs }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {

        // by default, we reserve a capability for each output port at `Default::default()`.
        let mut internal = Vec::new();
        for _ in 0 .. self.shape.outputs {
            internal.push(ChangeBatch::new_from(Default::default(), self.shape.peers as i64));
        }

        (self.summary.clone(), internal)
    }

    // initialize self.frontier antichains as indicated by hosting scope.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>,
                                       count_map: &mut [ChangeBatch<T>]) {
        (self.push_external)(count_map);
    }

    // update self.frontier antichains as indicated by hosting scope.
    fn push_external_progress(&mut self, count_map: &mut [ChangeBatch<T>]) {
        (self.push_external)(count_map);
    }

    // invoke user logic, propagate changes found in shared ChangeBatches.
    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<T>],
                                         internal: &mut [ChangeBatch<T>],
                                         produced: &mut [ChangeBatch<T>]) -> bool
    {
        (self.pull_internal)(consumed, internal, produced)
    }

    fn name(&self) -> String { self.shape.name.clone() }
    fn notify_me(&self) -> bool { self.shape.notify }
}
