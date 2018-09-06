//! Types to build operators with general shapes.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use ::Data;

use progress::{ChangeBatch, Timestamp};
use progress::frontier::{Antichain, MutableAntichain};

use dataflow::{Stream, Scope};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::operators::capability::Capability;
use dataflow::operators::capability::mint as mint_capability;

use dataflow::operators::generic::handles::{InputHandle, new_input_handle, OutputWrapper};

use logging::Logger;

use super::builder_raw::OperatorBuilder as OperatorBuilderRaw;

/// Builds operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    builder: OperatorBuilderRaw<G>,
    frontier: Vec<MutableAntichain<G::Timestamp>>,
    consumed: Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>>>,
    produced: Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>,
    logging: Option<Logger>,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, scope: G) -> Self {
        let logging = scope.logging();
        OperatorBuilder {
            builder: OperatorBuilderRaw::new(name, scope),
            frontier: Vec::new(),
            consumed: Vec::new(),
            internal: Rc::new(RefCell::new(Vec::new())),
            produced: Vec::new(),
            logging,
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

        let connection = vec![Antichain::from_elem(Default::default()); self.builder.shape().outputs()];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input with connection information to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input_connection<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> InputHandle<G::Timestamp, D, P::Puller>
        where
            P: ParallelizationContract<G::Timestamp, D> {

        let puller = self.builder.new_input_connection(stream, pact, connection);

        let input = PullCounter::new(puller);
        self.frontier.push(MutableAntichain::new());
        self.consumed.push(input.consumed().clone());

        new_input_handle(input, self.internal.clone(), self.logging.clone())
    }

    /// Adds a new output to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (OutputWrapper<G::Timestamp, D, Tee<G::Timestamp, D>>, Stream<G, D>) {
        let connection = vec![Antichain::from_elem(Default::default()); self.builder.shape().inputs()];
        self.new_output_connection(connection)
    }

    /// Adds a new output with connection information to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_output_connection<D: Data>(&mut self, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> (OutputWrapper<G::Timestamp, D, Tee<G::Timestamp, D>>, Stream<G, D>) {

        let (tee, stream) = self.builder.new_output_connection(connection);

        let internal = Rc::new(RefCell::new(ChangeBatch::new()));
        self.internal.borrow_mut().push(internal.clone());

        let mut buffer = PushBuffer::new(PushCounter::new(tee));
        self.produced.push(buffer.inner().produced().clone());

        (OutputWrapper::new(buffer, internal), stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<B, L>(self, constructor: B)
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: FnMut(&[MutableAntichain<G::Timestamp>])+'static
    {
        // create capabilities, discard references to their creation.
        let mut capabilities = Vec::new();
        for output_index in 0  .. self.internal.borrow().len() {
            let borrow = &self.internal.borrow()[output_index];
            capabilities.push(mint_capability(Default::default(), borrow.clone()));
            // Discard evidence of creation, as we are assumed to start with one.
            borrow.borrow_mut().clear();
        }

        let mut logic = constructor(capabilities);

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
            let self_internal_borrow = self_internal.borrow_mut();
            for index in 0 .. self_internal_borrow.len() {
                let mut borrow = self_internal_borrow[index].borrow_mut();
                internal[index].extend(borrow.drain());
            }

            // move batches of produced changes.
            for index in 0 .. produced.len() {
                self_produced[index].borrow_mut().drain_into(&mut produced[index]);
            }

            false
        };

        self.builder.build(pep, pip);
    }

    /// Get the identifier assigned to the operator being constructed
    pub fn index(&self) -> usize {
        self.builder.index()
    }
}


#[cfg(test)]
mod tests {

    #[test]
    #[should_panic]
    fn incorrect_capabilities() {

        // This tests that if we attempt to use a capability associated with the
        // wrong output, there is a run-time assertion.

        use ::dataflow::operators::generic::builder_rc::OperatorBuilder;

        ::example(|scope| {

            let mut builder = OperatorBuilder::new("Failure".to_owned(), scope.clone());

            // let mut input = builder.new_input(stream, Pipeline);
            let (mut output1, _stream1) = builder.new_output::<()>();
            let (mut output2, _stream2) = builder.new_output::<()>();

            builder.build(move |capabilities| {
                move |_frontiers| {

                    let mut output_handle1 = output1.activate();
                    let mut output_handle2 = output2.activate();

                    // NOTE: Using incorrect capabilities here.
                    output_handle2.session(&capabilities[0]);
                    output_handle1.session(&capabilities[1]);
                }
            });
        })
    }

    #[test]
    fn correct_capabilities() {

        // This tests that if we attempt to use capabilities with the correct outputs
        // there is no runtime assertion

        use ::dataflow::operators::generic::builder_rc::OperatorBuilder;

        ::example(|scope| {

            let mut builder = OperatorBuilder::new("Failure".to_owned(), scope.clone());

            // let mut input = builder.new_input(stream, Pipeline);
            let (mut output1, _stream1) = builder.new_output::<()>();
            let (mut output2, _stream2) = builder.new_output::<()>();

            builder.build(move |mut capabilities| {
                move |_frontiers| {

                    let mut output_handle1 = output1.activate();
                    let mut output_handle2 = output2.activate();

                    // Avoid second call.
                    if !capabilities.is_empty() {

                        // NOTE: Using correct capabilities here.
                        output_handle1.session(&capabilities[0]);
                        output_handle2.session(&capabilities[1]);

                        capabilities.clear();
                    }
                }
            });

            "Hello".to_owned()
        });
    }
}
