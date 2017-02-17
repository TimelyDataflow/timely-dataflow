
//! Methods to construct generic streaming and blocking unary operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use timely_communication::Push;
use dataflow::channels::message::Content;

use progress::nested::subgraph::{Source, Target};

use progress::count_map::CountMap;
use progress::{Timestamp, Operate, Antichain};
use progress::frontier::MutableAntichain;
use dataflow::channels::pushers::{Tee, TeeHelper};
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pact::ParallelizationContract;
use dataflow::channels::pullers::Counter as PullCounter;

use dataflow::operators::{InputHandle, FrontieredInputHandle, OutputHandle};
use dataflow::operators::handles::{new_input_handle, new_frontier_input_handle, new_output_handle};
use dataflow::operators::capability::Capability;
use dataflow::operators::capability::mint as mint_capability;

use ::Data;

use dataflow::{Stream, Scope};

/// Methods to construct generic streaming and blocking operators.
pub trait Operator<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic` which can read from the input stream, write
    /// to the output stream, and inspect the frontier at the input.
    /// `logic` can read from the input stream, and write to the output stream.
    ///
    /// #Examples
    /// ```
    /// extern crate timely;
    /// extern crate timely_communication;
    ///
    /// use std::collections::HashMap;
    ///
    /// use timely_communication::Configuration;
    ///
    /// use timely::dataflow::operators::{ToStream, Operator, FrontierNotificator};
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    /// use timely::dataflow::operators::{InputHandle, OutputHandle};
    /// use timely::progress::Antichain;
    /// use timely::dataflow::channels::pushers::Tee;
    /// use timely::dataflow::scopes::Root;
    /// use timely::dataflow::Scope;
    /// 
    /// fn main() {
    ///     timely::execute(Configuration::Thread, |root| {
    ///         root.scoped(|scope| {
    ///             (0u64..10).to_stream(scope)
    ///                 .unary_frontier(Pipeline, "example", |default_cap| {
    ///                     let mut cap = Some(default_cap.delayed(&RootTimestamp::new(12)));
    ///                     let mut notificator = FrontierNotificator::new();
    ///                     let mut stash = HashMap::new();
    ///                     move |input, output| {
    ///                         if let Some(ref c) = cap.take() {
    ///                             output.session(&c).give(12);
    ///                         }
    ///                         while let Some((time, data)) = input.next() {
    ///                             stash.entry(time.time()).or_insert(Vec::new());
    ///                         }
    ///                         for time in notificator.iter(&[input.frontier()]) {
    ///                             if let Some(mut vec) = stash.remove(&time.time()) {
    ///                                 output.session(&time).give_iterator(vec.drain(..));
    ///                             }
    ///                         }
    ///                     }
    ///                 });
    ///         });
    ///     });
    /// }
    /// ```
    fn unary_frontier<D2, B, L, P>(&self, pact: P, name: &str, building: B) -> Stream<G, D2>
    where
        D2: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1>, &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic`, the function returned by the function parameter.
    /// `logic` can read from the input stream, and write to the output stream.
    ///
    /// #Examples
    /// ```
    /// extern crate timely;
    /// extern crate timely_communication;
    ///
    /// use std::collections::HashMap;
    ///
    /// use timely_communication::Configuration;
    ///
    /// use timely::dataflow::operators::{ToStream, Operator, FrontierNotificator};
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    /// use timely::dataflow::operators::{InputHandle, OutputHandle};
    /// use timely::progress::Antichain;
    /// use timely::dataflow::channels::pushers::Tee;
    /// use timely::dataflow::scopes::Root;
    /// use timely::dataflow::Scope;
    /// 
    /// fn main() {
    ///     timely::execute(Configuration::Thread, |root| {
    ///         root.scoped(|scope| {
    ///             (0u64..10).to_stream(scope)
    ///                 .unary(Pipeline, "example", |default_cap| {
    ///                     let mut cap = Some(default_cap.delayed(&RootTimestamp::new(12)));
    ///                     move |input, output| {
    ///                         if let Some(ref c) = cap.take() {
    ///                             output.session(&c).give(12);
    ///                         }
    ///                         while let Some((time, data)) = input.next() {
    ///                             output.session(&time).give_content(data);
    ///                         }
    ///                     }
    ///                 });
    ///         });
    ///     });
    /// }
    /// ```
    fn unary<D2, B, L, P>(&self, pact: P, name: &str, building: B) -> Stream<G, D2>
    where
        D2: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1>, &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic` which can read from the input streams, write
    /// to the output stream, and inspect the frontier at the inputs.
    fn binary_frontier<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, building: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1>,
                 &mut FrontieredInputHandle<G::Timestamp, D2>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic` which can read from the input streams, and write
    /// to the output stream.
    fn binary<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, building: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1>, &mut InputHandle<G::Timestamp, D2>, &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;
}

#[inline]
fn make_default_cap<G: Scope>() -> (Capability<G::Timestamp>, Rc<RefCell<CountMap<G::Timestamp>>>) {
    let internal_changes = Rc::new(RefCell::new(CountMap::new()));
    let cap = mint_capability(Default::default(), internal_changes.clone());

    (cap, internal_changes)
}

#[inline]
fn unary_base<G: Scope, D1: Data, D2: Data, P>(
    slf: &Stream<G, D1>, pact: P) -> (
        PullCounter<G::Timestamp, D1>,
        (Box<Push<(G::Timestamp, Content<D1>)>>, usize),
        (Tee<G::Timestamp, D2>, TeeHelper<G::Timestamp, D2>),
        G)
    where
        P: ParallelizationContract<G::Timestamp, D1> {

    let mut scope = slf.scope();

    // input channel
    let channel_id = scope.new_identifier();
    let (sender, receiver) = pact.connect(&mut scope, channel_id);
    let input = PullCounter::new(receiver);

    let (targets, registrar) = Tee::<G::Timestamp,D2>::new();
    (input, (sender, channel_id), (targets, registrar), scope)
}

#[inline]
fn binary_base<G: Scope, D1: Data, D2: Data, D3: Data, P1, P2>(
    slf: &Stream<G, D1>, pact1: P1, pact2: P2) -> (
        PullCounter<G::Timestamp, D1>,
        (Box<Push<(G::Timestamp, Content<D1>)>>, usize),
        PullCounter<G::Timestamp, D2>,
        (Box<Push<(G::Timestamp, Content<D2>)>>, usize),
        (Tee<G::Timestamp, D3>, TeeHelper<G::Timestamp, D3>),
        G)
    where
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2> {

    let mut scope = slf.scope();

    // input channel
    let channel_id1 = scope.new_identifier();
    let channel_id2 = scope.new_identifier();
    let (sender1, receiver1) = pact1.connect(&mut scope, channel_id1);
    let (sender2, receiver2) = pact2.connect(&mut scope, channel_id2);
    let input1 = PullCounter::new(receiver1);
    let input2 = PullCounter::new(receiver2);

    let (targets, registrar) = Tee::<G::Timestamp, D3>::new();
    (input1, (sender1, channel_id1),
     input2, (sender2, channel_id2),
     (targets, registrar), scope)
}

impl<G: Scope, D1: Data> Operator<G, D1> for Stream<G, D1> {
    fn unary_frontier<D2, B, L, P>(&self, pact: P, name: &str, building: B) -> Stream<G, D2>
    where
        D2: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1>, &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1> {

        let (cap, internal_changes) = make_default_cap::<G>();
        let mut logic = building(cap);

        let (mut input, (sender, channel_id), (targets, registrar), mut scope) = unary_base(self, pact);

        let operator = OperatorImpl::new(
            name.to_owned(),
            1,
            scope.peers(),
            move |consumed, internal, frontiers, output| {
                {
                    let mut input_handle = new_frontier_input_handle(&mut input, internal, &frontiers[0]);
                    logic(&mut input_handle, output);
                }
                input.pull_progress(&mut consumed[0]);
            },
            internal_changes,
            targets,
            true);

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }

    fn unary<D2, B, L, P>(&self, pact: P, name: &str, building: B) -> Stream<G, D2>
    where
        D2: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1>, &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1> {

        let (cap, internal_changes) = make_default_cap::<G>();
        let mut logic = building(cap);

        let (mut input, (sender, channel_id), (targets, registrar), mut scope) = unary_base(self, pact);

        let operator = OperatorImpl::new(
            name.to_owned(),
            1,
            scope.peers(),
            move |consumed, internal, _, output| {
                {
                    let mut input_handle = new_input_handle(&mut input, internal);
                    logic(&mut input_handle, output);
                }
                input.pull_progress(&mut consumed[0]);
            },
            internal_changes,
            targets,
            true);

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }

    fn binary<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, building: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1>, &mut InputHandle<G::Timestamp, D2>, &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2> {

        let (cap, internal_changes) = make_default_cap::<G>();
        let mut logic = building(cap);

        let (mut input1, (sender1, channel_id1),
             mut input2, (sender2, channel_id2),
             (targets, registrar), mut scope) = binary_base(self, pact1, pact2);

        let operator = OperatorImpl::new(
            name.to_owned(),
            2,
            scope.peers(),
            move |consumed, internal, _, output| {
                {
                    let mut input_handle1 = new_input_handle(&mut input1, internal.clone());
                    let mut input_handle2 = new_input_handle(&mut input2, internal);
                    logic(&mut input_handle1, &mut input_handle2, output);
                }
                input1.pull_progress(&mut consumed[0]);
                input2.pull_progress(&mut consumed[1]);
            },
            internal_changes,
            targets,
            true);

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender1, channel_id1);
        other.connect_to(Target { index: index, port: 1 }, sender2, channel_id2);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)
    }

    fn binary_frontier<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, building: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: Fn(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1>,
                 &mut FrontieredInputHandle<G::Timestamp, D2>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2> {

        let (cap, internal_changes) = make_default_cap::<G>();
        let mut logic = building(cap);

        let (mut input1, (sender1, channel_id1),
             mut input2, (sender2, channel_id2),
             (targets, registrar), mut scope) = binary_base(self, pact1, pact2);

        let operator = OperatorImpl::new(
            name.to_owned(),
            2,
            scope.peers(),
            move |consumed, internal, frontiers, output| {
                {
                    let mut input_handle1 = new_frontier_input_handle(&mut input1, internal.clone(), &frontiers[0]);
                    let mut input_handle2 = new_frontier_input_handle(&mut input2, internal, &frontiers[1]);
                    logic(&mut input_handle1, &mut input_handle2, output);
                }
                input1.pull_progress(&mut consumed[0]);
                input2.pull_progress(&mut consumed[1]);
            },
            internal_changes,
            targets,
            true);

        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender1, channel_id1);
        other.connect_to(Target { index: index, port: 1 }, sender2, channel_id2);

        Stream::new(Source { index: index, port: 0 }, registrar, scope)

    }
}

// L: (consumed, internal, frontier, handle)
struct OperatorImpl<T: Timestamp,
                    L: FnMut(
                        &mut [CountMap<T>],
                        Rc<RefCell<CountMap<T>>>,
                        &[MutableAntichain<T>],
                        &mut OutputHandle<T, DO, Tee<T, DO>>),
                    DO: Data> {
    name:             String,
    input_count:      usize,
    peers:            usize,
    logic:            L,
    frontier:         Vec<MutableAntichain<T>>,
    internal_changes: Rc<RefCell<CountMap<T>>>,
    output:           PushBuffer<T, DO, PushCounter<T, DO, Tee<T, DO>>>,
    notify:           bool,
}

impl<T: Timestamp,
     L: FnMut(
         &mut [CountMap<T>],
         Rc<RefCell<CountMap<T>>>,
         &[MutableAntichain<T>],
         &mut OutputHandle<T, DO, Tee<T, DO>>),
     DO: Data>
OperatorImpl<T, L, DO> {

    fn new(name: String,
           input_count: usize,
           peers: usize,
           logic: L,
           internal_changes: Rc<RefCell<CountMap<T>>>,
           targets: Tee<T, DO>,
           notify: bool) -> OperatorImpl<T, L, DO> {
        OperatorImpl {
            name: name,
            input_count: input_count,
            peers: peers,
            logic: logic,
            frontier: (0..input_count).map(|_| MutableAntichain::new()).collect(),
            internal_changes: internal_changes,
            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            notify: notify,
        }
    }
}

fn update_frontiers<T: Timestamp>(frontiers: &mut Vec<MutableAntichain<T>>, count_maps: &mut [CountMap<T>]) {
    for (counts, frontier) in count_maps.iter_mut().zip(frontiers.iter_mut()) {
        while let Some((time, delta)) = counts.pop() {
            frontier.update(&time, delta);
        }
    }
}

impl<T: Timestamp,
     L: FnMut(
         &mut [CountMap<T>],
         Rc<RefCell<CountMap<T>>>,
         &[MutableAntichain<T>],
         &mut OutputHandle<T, DO, Tee<T, DO>>),
     DO: Data>
Operate<T> for OperatorImpl<T, L, DO> {

    fn inputs(&self) -> usize { self.input_count }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        let mut internal = vec![CountMap::new()];
        // augment the counts for each reserved capability.
        for &(ref time, count) in self.internal_changes.borrow().iter() {
            internal[0].update(time, count * (self.peers as i64 - 1));
        }

        // drain the changes to empty out, and complete the counts for internal.
        self.internal_changes.borrow_mut().drain_into(&mut internal[0]);

        let summary = (0..self.input_count).map(|_| vec![Antichain::from_elem(Default::default())]).collect::<Vec<_>>();
        (summary, internal)
    }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>,
                                       count_map: &mut [CountMap<T>]) {
        update_frontiers(&mut self.frontier, count_map);
    }

    fn push_external_progress(&mut self, count_map: &mut [CountMap<T>]) {
        update_frontiers(&mut self.frontier, count_map);
    }

    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<T>],
                                         internal: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool
    {
        {
            let mut output_handle = new_output_handle(&mut self.output);
            (self.logic)(consumed, self.internal_changes.clone(), &self.frontier[..], &mut output_handle);
        }

        self.output.cease();
        self.output.inner().pull_progress(&mut produced[0]);
        self.internal_changes.borrow_mut().drain_into(&mut internal[0]);

        false   // no unannounced internal work
    }

    fn name(&self) -> String { self.name.clone() }
    fn notify_me(&self) -> bool { self.notify }
}
