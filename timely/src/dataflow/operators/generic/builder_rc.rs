//! Types to build operators with general shapes.

use std::cell::RefCell;
use std::default::Default;
use std::rc::Rc;

use crate::Data;
use crate::dataflow::{Scope, Stream};
use crate::dataflow::channels::pact::ParallelizationContract;
use crate::dataflow::channels::pullers::Counter as PullCounter;
use crate::dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use crate::dataflow::channels::pushers::Counter as PushCounter;
use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::operators::capability::Capability;
use crate::dataflow::operators::capability::mint as mint_capability;
use crate::dataflow::operators::generic::builder_raw::OperatorCoreLogic;
use crate::dataflow::operators::generic::handles::{InputHandle, new_input_handle, OutputWrapper};
use crate::dataflow::operators::generic::operator_info::OperatorInfo;
use crate::logging::TimelyLogger as Logger;
use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::frontier::{Antichain, MutableAntichain};
use crate::progress::operate::SharedProgress;

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
    ///
    /// The `connection` parameter contains promises made by the operator for each of the existing *outputs*, that any timestamp
    /// appearing at the input, any output timestamp will be greater than or equal to the input timestamp subjected to a `Summary`
    /// greater or equal to some element of the corresponding antichain in `connection`.
    ///
    /// Commonly the connections are either the unit summary, indicating the same timestamp might be produced as output, or an empty
    /// antichain indicating that there is no connection from the input to the output.
    pub fn new_input_connection<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> InputHandle<G::Timestamp, D, P::Puller>
        where
            P: ParallelizationContract<G::Timestamp, D> {

        let puller = self.builder.new_input_connection(stream, pact, connection);

        let input = PullCounter::new(puller);
        self.frontier.push(MutableAntichain::new());
        self.consumed.push(input.consumed().clone());

        new_input_handle(input, self.internal.clone(), self.logging.clone())
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (OutputWrapper<G::Timestamp, D, Tee<G::Timestamp, D>>, Stream<G, D>) {
        let connection = vec![Antichain::from_elem(Default::default()); self.builder.shape().inputs()];
        self.new_output_connection(connection)
    }

    /// Adds a new output with connection information to a generic operator builder, returning the `Push` implementor to use.
    ///
    /// The `connection` parameter contains promises made by the operator for each of the existing *inputs*, that any timestamp
    /// appearing at the input, any output timestamp will be greater than or equal to the input timestamp subjected to a `Summary`
    /// greater or equal to some element of the corresponding antichain in `connection`.
    ///
    /// Commonly the connections are either the unit summary, indicating the same timestamp might be produced as output, or an empty
    /// antichain indicating that there is no connection from the input to the output.
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
        self.build_reschedule(|caps| {
            let mut logic = constructor(caps);
            move |frontier| { logic(frontier); false }
        })
    }

    /// Creates an operator implementation from supplied logic constructor.
    ///
    /// Unlike `build`, the supplied closure can indicate if the operator
    /// should be considered incomplete. The `build` method indicates that
    /// the operator is never incomplete and can be shut down at the system's
    /// discretion.
    pub fn build_reschedule<B, L>(self, constructor: B)
        where
            B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
            L: FnMut(&[MutableAntichain<G::Timestamp>])->bool+'static
    {
        self.build_reschedule_core(constructor)
    }

    /// Creates an operator implementation from supplied logic constructor.
    ///
    /// Unlike `build`, the supplied closure can indicate if the operator
    /// should be considered incomplete. The `build` method indicates that
    /// the operator is never incomplete and can be shut down at the system's
    /// discretion.
    pub fn build_reschedule_core<B, L>(self, constructor: B)
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: OperatorRcLogic<G::Timestamp>+'static
    {
        // create capabilities, discard references to their creation.
        let mut capabilities = Vec::new();
        for output_index in 0  .. self.internal.borrow().len() {
            let borrow = &self.internal.borrow()[output_index];
            capabilities.push(mint_capability(G::Timestamp::minimum(), borrow.clone()));
            // Discard evidence of creation, as we are assumed to start with one.
            borrow.borrow_mut().clear();
        }

        self.builder.build_core(OperatorRawLogic {
            frontier: self.frontier,
            consumed: self.consumed,
            internal: self.internal,
            produced: self.produced,
            logic: constructor(capabilities),
        });
    }

    /// Get the identifier assigned to the operator being constructed
    pub fn index(&self) -> usize {
        self.builder.index()
    }

    /// The operator's worker-unique identifier.
    pub fn global(&self) -> usize {
        self.builder.global()
    }

    /// Creates operator info for the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        self.builder.operator_info()
    }
}

/// Logic for an RC operator
pub trait OperatorRcLogic<T: Timestamp> {
    /// Notify the operator about a change in its inputs
    fn update(&mut self, progress: &[MutableAntichain<T>]) -> bool;
}

impl<T, F> OperatorRcLogic<T> for F
    where T: Timestamp, F: FnMut(&[MutableAntichain<T>]) -> bool {
    fn update(&mut self, progress: &[MutableAntichain<T>]) -> bool {
        (self)(progress)
    }
}

struct OperatorRawLogic<T: Timestamp, L: OperatorRcLogic<T> + 'static> {
    frontier: Vec<MutableAntichain<T>>,
    consumed: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>,
    produced: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    logic: L,
}

impl<T, L> OperatorCoreLogic<T> for OperatorRawLogic<T, L>
    where T: Timestamp, L: OperatorRcLogic<T> + 'static {
    fn update(&mut self, progress: &mut SharedProgress<T>) -> bool {
        // drain frontier changes
        for index in 0..progress.frontiers.len() {
            self.frontier[index].update_iter(progress.frontiers[index].drain());
        }

        // invoke supplied logic
        let result = self.logic.update(&self.frontier[..]);

        // move batches of consumed changes.
        for index in 0..progress.consumeds.len() {
            self.consumed[index].borrow_mut().drain_into(&mut progress.consumeds[index]);
        }

        // move batches of internal changes.
        let self_internal_borrow = self.internal.borrow_mut();
        for index in 0..self_internal_borrow.len() {
            let mut borrow = self_internal_borrow[index].borrow_mut();
            progress.internals[index].extend(borrow.drain());
        }

        // move batches of produced changes.
        for index in 0..progress.produceds.len() {
            self.produced[index].borrow_mut().drain_into(&mut progress.produceds[index]);
        }

        result
    }
}

#[cfg(test)]
mod tests {

    #[test]
    #[should_panic]
    fn incorrect_capabilities() {

        // This tests that if we attempt to use a capability associated with the
        // wrong output, there is a run-time assertion.

        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;

        crate::example(|scope| {

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

        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;

        crate::example(|scope| {

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
