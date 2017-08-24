//! Types to build operators with general shapes.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use ::Data;

use progress::nested::subgraph::{Source, Target};
use progress::ChangeBatch;
use progress::{Timestamp, Operate, Antichain};
use progress::frontier::MutableAntichain;

use dataflow::{Stream, Scope};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::operators::capability::Capability;
use dataflow::operators::capability::mint as mint_capability;

use dataflow::operators::generic::handles::{InputHandle, new_input_handle};

struct OperatorShape<T: Timestamp> {
    name: String,
    notify: bool,
    index: usize,
    peers: usize,
    frontier: Vec<MutableAntichain<T>>,
    consumed: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    internal: Rc<RefCell<ChangeBatch<T>>>,
    produced: Vec<Rc<RefCell<ChangeBatch<T>>>>,
}

/// Core data for the structure of an operator, minus scope and logic.
impl<T: Timestamp> OperatorShape<T> {
    fn new(name: String, notify: bool, index: usize, peers: usize) -> Self {
        OperatorShape {
            name: name,
            notify: notify,
            index: index,   // operator index, *not* worker/scope index.
            peers: peers,
            frontier: Vec::new(),
            consumed: Vec::new(),
            internal: Rc::new(RefCell::new(ChangeBatch::new())),
            produced: Vec::new(),            
        }
    }
}

/// Builds operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    scope: G,
    shape: OperatorShape<G::Timestamp>,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, notify: bool, mut scope: G) -> Self {
        let index = scope.allocate_operator_index();
        let peers = scope.peers();

        OperatorBuilder {
            scope: scope,
            shape: OperatorShape::new(name, notify, index, peers),
        }
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P) -> InputHandle<G::Timestamp, D, P::Puller>
    where
        P: ParallelizationContract<G::Timestamp, D> {

        let channel_id = self.scope.new_identifier();
        let (sender, receiver) = pact.connect(&mut self.scope, channel_id);
        let input = PullCounter::new(receiver);

        let target = Target { index: self.shape.index, port: self.shape.consumed.len() };
        stream.connect_to(target, sender, channel_id);
        
        self.shape.frontier.push(MutableAntichain::new());
        self.shape.consumed.push(input.consumed().clone());

        new_input_handle(input, self.shape.internal.clone())
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (PushBuffer<G::Timestamp, D, PushCounter<G::Timestamp, D, Tee<G::Timestamp, D>>>, Stream<G, D>) {

        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let mut buffer = PushBuffer::new(PushCounter::new(targets));

        let source = Source { index: self.shape.index, port: self.shape.produced.len() };
        let stream = Stream::new(source, registrar, self.scope.clone());

        self.shape.produced.push(buffer.inner().produced().clone());

        (buffer, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<B, L>(self, constructor: B) 
    where 
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&[MutableAntichain<G::Timestamp>])+'static
    {
        let mut scope = self.scope;
        let index = self.shape.index;

        let cap = mint_capability(Default::default(), self.shape.internal.clone());
        self.shape.internal.borrow_mut().clear();

        let logic = constructor(cap);

        let operator = OperatorCore {
            shape: self.shape,
            logic: logic,
        };

        scope.add_operator_with_index(operator, index);
    }
}

struct OperatorCore<T, L> 
    where 
        T: Timestamp,
        L: FnMut(&[MutableAntichain<T>])
{
    shape: OperatorShape<T>,
    logic: L,
}

impl<T, L> Operate<T> for OperatorCore<T, L> 
    where 
        T: Timestamp,
        L: FnMut(&[MutableAntichain<T>])
{
    fn inputs(&self) -> usize { self.shape.consumed.len() }
    fn outputs(&self) -> usize { self.shape.produced.len() }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {

        // by default, we reserve a capability for each output port at `Default::default()`.
        let mut internal = Vec::new();
        for _ in 0 .. self.shape.produced.len() {
            internal.push(ChangeBatch::new_from(Default::default(), self.shape.peers as i64));
        }

        let summary = vec![vec![Antichain::from_elem(Default::default()); self.shape.produced.len()]; self.shape.consumed.len()];

        (summary, internal)
    }

    // initialize self.frontier antichains as indicated by hosting scope.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>,
                                       count_map: &mut [ChangeBatch<T>]) {
        assert_eq!(count_map.len(), self.shape.frontier.len());
        for index in 0 .. count_map.len() {
            self.shape.frontier[index].update_iter(count_map[index].drain());
        }
    }

    // update self.frontier antichains as indicated by hosting scope.
    fn push_external_progress(&mut self, count_map: &mut [ChangeBatch<T>]) {
        for index in 0 .. count_map.len() {
            self.shape.frontier[index].update_iter(count_map[index].drain());
        }
    }

    // invoke user logic, propagate changes found in shared ChangeBatches.
    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<T>],
                                         internal: &mut [ChangeBatch<T>],
                                         produced: &mut [ChangeBatch<T>]) -> bool
    {
        // invoke supplied logic
        (self.logic)(&self.shape.frontier);

        // move batches of consumed changes.
        for index in 0 .. consumed.len() {
            self.shape.consumed[index].borrow_mut().drain_into(&mut consumed[index]);
        }

        // move batches of internal changes.
        for index in 0 .. internal.len() {
            let mut borrow = self.shape.internal.borrow_mut();
            internal[index].extend(borrow.iter().cloned());
        }
        self.shape.internal.borrow_mut().clear();

        // move batches of produced changes.
        for index in 0 .. produced.len() {
            self.shape.produced[index].borrow_mut().drain_into(&mut produced[index]);
        }

        false   // no unannounced internal work
    }

    fn name(&self) -> String { self.shape.name.clone() }
    fn notify_me(&self) -> bool { self.shape.notify }
}