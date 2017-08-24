//! Types to build operators with general shapes.


use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use timely_communication::{Push, Pull};

use ::Data;

use progress::nested::subgraph::{Source, Target};
use progress::ChangeBatch;
use progress::{Timestamp, Operate, Antichain};

use progress::frontier::MutableAntichain;

use dataflow::channels::Content;

use dataflow::{Stream, Scope};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::operators::capability::Capability;
use dataflow::operators::capability::mint as mint_capability;

struct OperatorShape<T: Timestamp> {
    name: String,
    notify: bool,
    index: usize,
    peers: usize,
    frontier: Vec<MutableAntichain<T>>,
    internal: Rc<RefCell<ChangeBatch<T>>>,
    outputs: usize,
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
            internal: Rc::new(RefCell::new(ChangeBatch::new())),
            outputs: 0,
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
    pub fn new_input<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P) -> Box<Pull<(G::Timestamp, Content<D>)>>
    where
        P: ParallelizationContract<G::Timestamp, D> {

        let channel_id = self.scope.new_identifier();
        let (sender, receiver) = pact.connect(&mut self.scope, channel_id);

        let target = Target { index: self.shape.index, port: self.shape.frontier.len() };
        stream.connect_to(target, sender, channel_id);
        
        self.shape.frontier.push(MutableAntichain::new());

        receiver
    }

    /// Adds a new input to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (Tee<G::Timestamp, D>, Stream<G, D>) {

        let (targets, registrar) = Tee::<G::Timestamp,D>::new();

        let source = Source { index: self.shape.index, port: self.shape.outputs };
        let stream = Stream::new(source, registrar, self.scope.clone());

        self.shape.outputs += 1;

        (targets, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<B, L>(self, constructor: B) 
    where 
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(
            &[MutableAntichain<G::Timestamp>],
            &mut [ChangeBatch<G::Timestamp>],
            &Rc<RefCell<ChangeBatch<G::Timestamp>>>,
            &mut [ChangeBatch<G::Timestamp>],
        )+'static
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
        L: FnMut(
            &[MutableAntichain<T>],
            &mut [ChangeBatch<T>],
            &Rc<RefCell<ChangeBatch<T>>>,
            &mut [ChangeBatch<T>],
        )
{
    shape: OperatorShape<T>,
    logic: L,
}

impl<T, L> Operate<T> for OperatorCore<T, L> 
    where 
        T: Timestamp,
        L: FnMut(
            &[MutableAntichain<T>],
            &mut [ChangeBatch<T>],
            &Rc<RefCell<ChangeBatch<T>>>,
            &mut [ChangeBatch<T>],
        )
{
    fn inputs(&self) -> usize { self.shape.frontier.len() }
    fn outputs(&self) -> usize { self.shape.outputs }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {

        // by default, we reserve a capability for each output port at `Default::default()`.
        let mut internal = Vec::new();
        for _ in 0 .. self.shape.outputs {
            internal.push(ChangeBatch::new_from(Default::default(), self.shape.peers as i64));
        }

        let summary = vec![vec![Antichain::from_elem(Default::default()); self.shape.outputs]; self.shape.frontier.len()];

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
        (self.logic)(&self.shape.frontier, consumed, &self.shape.internal, produced);

        // move batches of internal changes.
        let mut borrow = self.shape.internal.borrow_mut();
        for index in 1 .. internal.len() {
            internal[index].extend(borrow.iter().cloned());
        }
        internal[0].extend(borrow.drain());

        false   // no unannounced internal work
    }

    fn name(&self) -> String { self.shape.name.clone() }
    fn notify_me(&self) -> bool { self.shape.notify }
}

/// Owned state needed for buffering pusher.
pub struct OutputAllocation<T, D, P: Push<(T, Content<D>)>> {
    /// .
    pub time: Option<T>,
    /// .
    pub buffer: Vec<D>,
    /// .
    pub pusher: P,
}

// We also require a few helpful input handles:
mod handles {

    use std::rc::Rc;
    use std::cell::RefCell;

    use timely_communication::{Push, Pull};

    use order::PartialOrder;
    use progress::ChangeBatch;
    use progress::frontier::MutableAntichain;
    use dataflow::channels::Content;

    use super::OutputAllocation;

    struct InputHandle<'a, T: Ord+Clone+'a, D, P: Pull<(T, Content<D>)>+'a> {
        consumed: &'a mut ChangeBatch<T>,
        internal: &'a Rc<RefCell<ChangeBatch<T>>>,
        puller: &'a mut P,
        phantom: ::std::marker::PhantomData<D>,
    }

    impl<'a, T: Ord+Clone+'a, D, P: Pull<(T, Content<D>)>+'a> InputHandle<'a, T, D, P> {

        fn new(
            consumed: &'a mut ChangeBatch<T>,
            internal: &'a Rc<RefCell<ChangeBatch<T>>>,
            puller: &'a mut P) -> Self {

            InputHandle {
                consumed: consumed,
                internal: internal,
                puller: puller,
                phantom: ::std::marker::PhantomData,
            }
        }

        /// Retrieves the next timestamp and batch of data.
        #[inline]
        pub fn next(&mut self) -> Option<(&T, &mut Content<D>)> {
            if let Some((ref time, ref mut data)) = *self.puller.pull() {
                // TODO: This ignores empty allocations, which might be wrong.
                //       We may want to return them as part of zero-copy.
                if data.len() > 0 {
                    self.consumed.update(time.clone(), data.len() as i64);
                    Some((time, data))
                }
                else { None }
            }
            else { None }
        }
    }

    struct FrontieredInputHandle<'a, T: PartialOrder+Ord+Clone+'a, D, P: Pull<(T, Content<D>)>+'a> {
        input: InputHandle<'a, T, D, P>,
        pub frontier: &'a MutableAntichain<T>,
    }

    impl<'a, T: PartialOrder+Ord+Clone+'a, D, P: Pull<(T, Content<D>)>+'a> FrontieredInputHandle<'a, T, D, P> {

        fn new(input: InputHandle<'a, T, D, P>, frontier: &'a MutableAntichain<T>) -> Self {
            FrontieredInputHandle {
                input: input,
                frontier: frontier,
            }
        }

        /// Retrieves the next timestamp and batch of data.
        #[inline]
        pub fn next(&mut self) -> Option<(&T, &mut Content<D>)> {
            self.input.next()
        }
    }

    struct OutputHandle<'a, T: Ord+Clone+'a, D: 'a, P: Push<(T, Content<D>)>+'a> {
        internal: &'a Rc<RefCell<ChangeBatch<T>>>,  // to validate capabilities. (?)
        produced: &'a mut ChangeBatch<T>,
        pusher: &'a mut OutputAllocation<T, D, P>,
    }

    impl<'a, T: Ord+Clone+'a, D: 'a, P: Push<(T, Content<D>)>+'a> OutputHandle<'a, T, D, P> {
        // copy/paste buffer.rs?
    }
}