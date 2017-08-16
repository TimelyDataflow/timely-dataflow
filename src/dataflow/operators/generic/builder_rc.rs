//! Types to build operators with general shapes.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use ::Data;

use progress::ChangeBatch;
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

use logging::Logger;

use super::builder_raw::OperatorBuilder as OperatorBuilderRaw;

/// Builds operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    builder: OperatorBuilderRaw<G>,
    frontier: Vec<MutableAntichain<G::Timestamp>>,
    consumed: Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>,
    internal: Rc<RefCell<ChangeBatch<G::Timestamp>>>,
    produced: Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>,
    logging: Logger,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, scope: G) -> Self {
        let logging = scope.logging();
        OperatorBuilder {
            builder: OperatorBuilderRaw::new(name, scope),
            frontier: Vec::new(),
            consumed: Vec::new(),
            internal: Rc::new(RefCell::new(ChangeBatch::new())),
            produced: Vec::new(),            
            logging: logging,
        }
    }

    /// Indicates whether the operator requires frontier information.
    pub fn set_notify(&mut self, notify: bool) {
        self.builder.set_notify(notify);
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P) -> InputHandle<G::Timestamp, D, P::Puller>
    where
        P: ParallelizationContract<G::Timestamp, D> {

        let puller = self.builder.new_input(stream, pact);

        let input = PullCounter::new(puller);
        self.frontier.push(MutableAntichain::new());
        self.consumed.push(input.consumed().clone());

        new_input_handle(input, self.internal.clone(), self.logging.clone())
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (PushBuffer<G::Timestamp, D, PushCounter<G::Timestamp, D, Tee<G::Timestamp, D>>>, Stream<G, D>) {

        let (tee, stream) = self.builder.new_output();

        let mut buffer = PushBuffer::new(PushCounter::new(tee));
        self.produced.push(buffer.inner().produced().clone());

        (buffer, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<B, L>(self, constructor: B) 
    where 
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&[MutableAntichain<G::Timestamp>])+'static
    {
        // create a capability, but discard any reference to its creation.
        let cap = mint_capability(Default::default(), self.internal.clone());
        self.internal.borrow_mut().clear();

        let mut logic = constructor(cap);

        let self_frontier1 = Rc::new(RefCell::new(self.frontier));
        let self_frontier2 = self_frontier1.clone();
        let self_consumed = self.consumed;
        let self_internal = self.internal;
        let self_produced = self.produced;

        let pep = move |changes: &mut [ChangeBatch<G::Timestamp>]| {
            let mut borrow = self_frontier1.borrow_mut();
            for index in 0 .. changes.len() {
                borrow[index].update_iter(changes[index].drain());
            }
        };

        let pip = move |consumed: &mut [ChangeBatch<G::Timestamp>], 
                        internal: &mut [ChangeBatch<G::Timestamp>], 
                        produced: &mut [ChangeBatch<G::Timestamp>]| {

            // invoke supplied logic 
            let borrow = self_frontier2.borrow();
            logic(&*borrow);

            // move batches of consumed changes.
            for index in 0 .. consumed.len() {
                self_consumed[index].borrow_mut().drain_into(&mut consumed[index]);
            }

            // move batches of internal changes.
            for index in 0 .. internal.len() {
                let mut borrow = self_internal.borrow_mut();
                internal[index].extend(borrow.iter().cloned());
            }
            self_internal.borrow_mut().clear();

            // move batches of produced changes.
            for index in 0 .. produced.len() {
                self_produced[index].borrow_mut().drain_into(&mut produced[index]);
            }

            false
        };

        self.builder.build(pep, pip);
    }
}
