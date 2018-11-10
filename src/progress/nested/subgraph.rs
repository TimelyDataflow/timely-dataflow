//! A dataflow subgraph
//!
//! Timely dataflow graphs can be nested hierarchically, where some region of
//! graph is grouped, and presents upwards as an operator. This grouping needs
//! some care, to make sure that the presented operator reflects the behavior
//! of the grouped operators.

use std::rc::Rc;
use std::cell::RefCell;

use logging::TimelyLogger as Logger;

use progress::frontier::{Antichain, MutableAntichain, MutableAntichainFilter};
use progress::{Timestamp, Operate, operate::{Schedule, SharedProgress}};
use progress::{Location, Port, Source, Target};

use progress::ChangeBatch;
use progress::broadcast::Progcaster;
use progress::nested::reachability;
// use progress::nested::reachability_neu as reachability;
use progress::timestamp::Refines;

// IMPORTANT : by convention, a child identifier of zero is used to indicate inputs and outputs of
// the Subgraph itself. An identifier greater than zero corresponds to an actual child, which can
// be found at position (id - 1) in the `children` field of the Subgraph.

/// A builder for interactively initializing a `Subgraph`.
///
/// This collects all the information necessary to get a `Subgraph` up and
/// running, and is important largely through its `build` method which
/// actually creates a `Subgraph`.
pub struct SubgraphBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp,
{
    /// The name of this subgraph.
    pub name: String,

    /// A sequence of integers uniquely identifying the subgraph.
    pub path: Vec<usize>,

    /// The index assigned to the subgraph by its parent.
    index: usize,

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<TInner>>,
    child_count: usize,

    edge_stash: Vec<(Source, Target)>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<ChangeBatch<TInner>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    /// Logging handle
    logging: Option<Logger>,
}

impl<TOuter, TInner> SubgraphBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    /// Allocates a new input to the subgraph and returns the target to that input in the outer graph.
    pub fn new_input(&mut self, shared_counts: Rc<RefCell<ChangeBatch<TInner>>>) -> Target {
        self.input_messages.push(shared_counts);
        Target { index: self.index, port: self.input_messages.len() - 1 }
    }

    /// Allocates a new output from the subgraph and returns the source of that output in the outer graph.
    pub fn new_output(&mut self) -> Source {
        self.output_capabilities.push(MutableAntichain::new());
        Source { index: self.index, port: self.output_capabilities.len() - 1 }
    }

    /// Introduces a dependence from the source to the target.
    ///
    /// This method does not effect data movement, but rather reveals to the progress tracking infrastructure
    /// that messages produced by `source` should be expected to be consumed at `target`.
    pub fn connect(&mut self, source: Source, target: Target) {
        self.edge_stash.push((source, target));
    }

    /// Creates a new Subgraph from a channel allocator and "descriptive" indices.
    pub fn new_from(
        index: usize,
        mut path: Vec<usize>,
        logging: Option<Logger>,
        name: &str,
    )
        -> SubgraphBuilder<TOuter, TInner>
    {
        path.push(index);

        // Put an empty placeholder for "outer scope" representative.
        let children = vec![PerOperatorState::empty(0, 0)];

        SubgraphBuilder {
            name: name.to_owned(),
            path,
            index,
            children,
            child_count: 1,
            edge_stash: Vec::new(),
            input_messages: Vec::new(),
            output_capabilities: Vec::new(),
            logging,
        }
    }

    /// Allocates a new child identifier, for later use.
    pub fn allocate_child_id(&mut self) -> usize {
        self.child_count += 1;
        self.child_count - 1
    }

    /// Adds a new child to the subgraph.
    pub fn add_child(&mut self, child: Box<Operate<TInner>>, index: usize, identifier: usize) {
        {
            let mut child_path = self.path.clone();
            child_path.push(index);
            self.logging.as_mut().map(|l| l.log(::logging::OperatesEvent {
                id: identifier,
                addr: child_path,
                name: child.name().to_owned(),
            }));
        }
        self.children.push(PerOperatorState::new(child, index, self.path.clone(), identifier, self.logging.clone()))
    }

    /// Now that initialization is complete, actually build a subgraph.
    pub fn build<A: ::worker::AsWorker>(mut self, worker: &mut A) -> Subgraph<TOuter, TInner> {
        // at this point, the subgraph is frozen. we should initialize any internal state which
        // may have been determined after construction (e.g. the numbers of inputs and outputs).
        // we also need to determine what to return as a summary and initial capabilities, which
        // will depend on child summaries and capabilities, as well as edges in the subgraph.

        // perhaps first check that the children are sanely identified
        self.children.sort_by(|x,y| x.index.cmp(&y.index));
        assert!(self.children.iter().enumerate().all(|(i,x)| i == x.index));

        let inputs = self.input_messages.len();
        let outputs = self.output_capabilities.len();

        // Create empty child zero represenative.
        self.children[0] = PerOperatorState::empty(outputs, inputs);

        let mut builder = reachability::Builder::new();

        // Child 0 has `inputs` outputs and `outputs` inputs, not yet connected.
        builder.add_node(0, outputs, inputs, vec![vec![Antichain::new(); inputs]; outputs]);
        for (index, child) in self.children.iter().enumerate().skip(1) {
            builder.add_node(index, child.inputs, child.outputs, child.gis_summary.clone());
        }

        for (source, target) in self.edge_stash {
            self.children[source.index].edges[source.port].push(target);
            builder.add_edge(source, target);
        }

        let tracker = reachability::Tracker::allocate_from(builder.summarize());

        let progcaster = Progcaster::new(worker, &self.path, self.logging.clone());

        Subgraph {
            name: self.name,
            path: self.path,
            inputs,
            outputs,
            children: self.children,
            input_messages: self.input_messages,
            output_capabilities: self.output_capabilities,

            local_pointstamp: ChangeBatch::new(),
            final_pointstamp: ChangeBatch::new(),
            progcaster,
            pointstamp_builder: builder,
            pointstamp_tracker: tracker,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),

        }
    }
}


/// A dataflow subgraph.
///
/// The subgraph type contains the infrastructure required to describe the topology of and track
/// progress within a dataflow subgraph.
pub struct Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{

    name: String,           // a helpful name (often "Subgraph").
    /// Path of identifiers from the root.
    pub path: Vec<usize>,
    inputs: usize,          // number of inputs.
    outputs: usize,         // number of outputs.

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<TInner>>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<ChangeBatch<TInner>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    // pointstamp messages to exchange. ultimately destined for `messages` or `internal`.
    local_pointstamp: ChangeBatch<(Location, TInner)>,
    final_pointstamp: ChangeBatch<(Location, TInner)>,

    // Graph structure and pointstamp tracker.
    pointstamp_builder: reachability::Builder<TInner>,
    pointstamp_tracker: reachability::Tracker<TInner>,

    // channel / whatever used to communicate pointstamp updates to peers.
    progcaster: Progcaster<TInner>,

    shared_progress: Rc<RefCell<SharedProgress<TOuter>>>,
}

impl<TOuter, TInner> Schedule for Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{

    fn name(&self) -> &str { &self.name }

    fn path(&self) -> &[usize] { &self.path }

    fn schedule(&mut self) -> bool {

        // This method performs several actions related to progress tracking
        // and child operator scheduling. The actions have been broken apart
        // into atomic actions that should be able to be safely executed in
        // isolation, by a potentially clueless user (yours truly).
        //
        // The main atomicity requirement is that `self.commit_pointstamps()`
        // should not be broken apart, but this has been done for you!

        self.accept_frontier();     // Accept supplied frontier changes.
        self.harvest_inputs();      // Count records entering the scope.

        // These next steps have some flexibility, in that we do not *need*
        // to exchange progress information, nor do we *need* to drain the
        // results into `self.final_pointstamp`. However, if we do either
        // of these then we *MUST* exchange and drain entire sets of changes.
        //
        // Progress tracking correctness relies on the atomic exchange of batches.
        // Progress tracking liveness relies on the eventual exchange of batches.
        self.progcaster.send_and_recv(&mut self.local_pointstamp);
        self.local_pointstamp.drain_into(&mut self.final_pointstamp);

        self.commit_pointstamps();  // Commit and propagate final pointstamps.

        // Schedule child operators.
        //
        // We should be able to schedule arbitrary subsets of children, as
        // long as we eventually schedule all children that need to do work.
        let mut any_child_active = false;
        for index in 1 .. self.children.len() {
            let child_active = self.activate_child(index);
            any_child_active = any_child_active || child_active;
        }

        // Active iff any active child or outstanding capabilities, messages.
        any_child_active || self.pointstamp_tracker.tracking_anything()
    }

}


impl<TOuter, TInner> Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    /// Schedules a child operator and collects progress statements.
    ///
    /// The return value indicates that the child task cannot yet shut down.
    fn activate_child(&mut self, child_index: usize) -> bool {

        // Activate the child and harvest its progress updates. Here we pass along a reference
        // to the source in the progress tracker so that the child can determine if it holds
        // any capabilities; if not, it is a candidate for being shut down.

        let (targets, sources, frontier, _pushed) = self.pointstamp_tracker.node_state(child_index);
        let active = self.children[child_index].schedule(targets, sources, frontier);
        debug_assert!(_pushed.iter_mut().all(|x| x.is_empty()));

        // Extract progress statements into either pre- or post-exchange buffers.
        if self.children[child_index].local {
            self.children[child_index].extract_progress(&mut self.local_pointstamp);
        }
        else {
            self.children[child_index].extract_progress(&mut self.final_pointstamp);
        }

        active
    }

    /// Move frontier changes from parent into progress statements.
    fn accept_frontier(&mut self) {
        for (port, changes) in self.shared_progress.borrow_mut().frontiers.iter_mut().enumerate() {
            let source = Source { index: 0, port };
            for (time, value) in changes.drain() {
                self.pointstamp_tracker.update_source(
                    source,
                    TInner::to_inner(time),
                    value
                );
            }
        }
    }

    /// Collects counts of records entering the scope.
    ///
    /// This method moves message counts from the output of child zero to the inputs to
    /// attached operators. This is a bit of a hack, because normally one finds capabilities
    /// at an operator output, rather than message counts. These counts are used only at
    /// mark [XXX] where they are reported upwards to the parent scope.
    fn harvest_inputs(&mut self) {
        for input in 0 .. self.inputs {
            let source = Location::new_source(0, input);
            let mut borrowed = self.input_messages[input].borrow_mut();
            for (time, delta) in borrowed.drain() {
                for target in &self.children[0].edges[input] {
                    self.local_pointstamp.update((Location::from(*target), time.clone()), delta);
                }
                self.local_pointstamp.update((source, time), -delta);
            }
        }
    }

    /// Commits pointstamps in `self.final_pointstamp`.
    ///
    /// This method performs several steps that for reasons of correctness must
    /// be performed atomically, before control is returned again. These are:
    ///
    /// 1. Changes to child zero outputs are reported as consumed messages.
    /// 2. Changes to child zero inputs are reported as produced messages.
    /// 3. Frontiers for child zero inputs are reported as internal capabilities.
    ///
    /// Perhaps importantly, the frontiers for child zero are determined *without*
    /// the messages that are produced for child zero inputs, as we only want to
    /// report retained internal capabilities, and not now-external messages.
    ///
    /// In the course of propagating progress changes, we also propagate progress
    /// changes for all of the managed child operators.
    fn commit_pointstamps(&mut self) {

        // Process exchanged pointstamps. Handle child 0 statements carefully.
        for ((location, timestamp), delta) in self.final_pointstamp.drain() {

            // Child 0 corresponds to the parent scope and has special handling.
            if location.node == 0 {
                match location.port {
                    // [XXX] Report child 0's capabilities as consumed messages.
                    //       Note the re-negation of delta, to make counts positive.
                    Port::Source(scope_input) => {
                        self.shared_progress
                            .borrow_mut()
                            .consumeds[scope_input]
                            .update(timestamp.to_outer(), -delta);
                    },
                    // [YYY] Report child 0's input messages as produced messages.
                    //       Do not otherwise record, as we will not see subtractions,
                    //       and we do not want to present their implications upward.
                    Port::Target(scope_output) => {
                        self.shared_progress
                            .borrow_mut()
                            .produceds[scope_output]
                            .update(timestamp.to_outer(), delta);
                    },
                }
            }
            else {
                self.pointstamp_tracker.update(location, timestamp, delta);
            }
        }

        // Propagate implications of progress changes.
        self.pointstamp_tracker.propagate_all();

        // Drain propagated information into shared progress structure.
        for child in 0 .. self.children.len() {
            let frontier = &mut self.children[child].shared_progress.borrow_mut().frontiers[..];
            self.pointstamp_tracker
                .pushed_mut(child)
                .iter_mut()
                .enumerate()
                .for_each(|(output, pointstamps)| pointstamps.drain_into(&mut frontier[output]));
        }

        // Extract child zero frontier changes and report as internal capability changes.
        // This could have been fused with the previous step, with a special case for child zero;
        // this was not done in anticipation of event-driven progress statement reports.
        for (output, internal) in self.shared_progress.borrow_mut().internals.iter_mut().enumerate() {
            let frontiers = &mut self.children[0].shared_progress.borrow_mut().frontiers[..];
            frontiers[output]
                .drain()
                .map(|(time, diff)| (time.to_outer(), diff))
                .filter_through(&mut self.output_capabilities[output])
                .for_each(|(time, diff)| internal.update(time, diff));
        }
    }
}


impl<TOuter, TInner> Operate<TOuter> for Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    fn local(&self) -> bool { false }
    fn inputs(&self)  -> usize { self.inputs }
    fn outputs(&self) -> usize { self.outputs }

    // produces connectivity summaries from inputs to outputs, and reports initial internal
    // capabilities on each of the outputs (projecting capabilities from contained scopes).
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Rc<RefCell<SharedProgress<TOuter>>>) {

        // double-check that child 0 (the outside world) is correctly shaped.
        assert_eq!(self.children[0].outputs, self.inputs());
        assert_eq!(self.children[0].inputs, self.outputs());

        // Step 1:  Our topology is now fixed, so we can establish reachability.
        let mut pointstamp_summaries = self.pointstamp_builder.summarize();

        //          This operator's summaries are the child zero output -> input summaries.
        let mut internal_summary = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];
        for input in 0..self.inputs() {
            for &(target, ref antichain) in &pointstamp_summaries.source_target[0][input] {
                if target.index == 0 {
                    for summary in antichain.elements().iter() {
                        internal_summary[input][target.port].insert(TInner::summarize(summary.clone()));
                    };
                }
            }
        }

        //          We should now discard child zero to zero summaries, to avoid tracking them.
        for summaries in pointstamp_summaries.target_target[0].iter_mut() { summaries.retain(|&(t, _)| t.index > 0); }
        for summaries in pointstamp_summaries.source_target[0].iter_mut() { summaries.retain(|&(t, _)| t.index > 0); }
        //          We also discard summaries to children that do not require notification.
        for child in 0 .. self.children.len() {
            for summaries in pointstamp_summaries.target_target[child].iter_mut() { summaries.retain(|&(t,_)| self.children[t.index].notify); }
            for summaries in pointstamp_summaries.source_target[child].iter_mut() { summaries.retain(|&(t,_)| self.children[t.index].notify); }
        }

        //          Allocate the pointstamp tracker using the finalized topology.
        self.pointstamp_tracker = reachability::Tracker::allocate_from(pointstamp_summaries);


        // Step 2:  Each child has expressed initial capabilities (their `shared_progress.internals`).
        //          We introduce these into the progress tracker to determine the scope's initial
        //          internal capabilities.
        for child in self.children.iter_mut() {
            child.extract_progress(&mut self.final_pointstamp);
        }

        self.commit_pointstamps();  // Propagate expressed capabilities to output frontiers.

        // Step 3:  Return summaries and shared progress information.
        (internal_summary, self.shared_progress.clone())
    }

    fn set_external_summary(&mut self) {
        self.commit_pointstamps();  // ensure propagation of input frontiers.
        self.children
            .iter_mut()
            .flat_map(|child| child.operator.as_mut())
            .for_each(|op| op.set_external_summary());
    }
}

struct PerOperatorState<T: Timestamp> {

    name: String,       // name of the operator
    // addr: Vec<usize>,   // address of the operator
    index: usize,       // index of the operator within its parent scope
    id: usize,          // worker-unique identifier

    local: bool,        // indicates whether the operator will exchange data or not
    notify: bool,

    inputs: usize,      // number of inputs to the operator
    outputs: usize,     // number of outputs from the operator

    recently_active: bool,

    operator: Option<Box<Operate<T>>>,

    edges: Vec<Vec<Target>>,    // edges from the outputs of the operator

    shared_progress: Rc<RefCell<SharedProgress<T>>>,

    gis_summary: Vec<Vec<Antichain<T::Summary>>>,   // cached result from get_internal_summary.

    logging: Option<Logger>,
}

impl<T: Timestamp> PerOperatorState<T> {

    fn empty(inputs: usize, outputs: usize) -> PerOperatorState<T> {
        PerOperatorState {
            name:       "External".to_owned(),
            operator:   None,
            index:      0,
            id:         usize::max_value(),
            local:      false,
            inputs,
            outputs,

            recently_active: true,
            notify:     true,

            edges: vec![Vec::new(); outputs],

            logging: None,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs,outputs))),
            gis_summary: Vec::new(),
        }
    }

    pub fn new(mut scope: Box<Operate<T>>, index: usize, mut _path: Vec<usize>, identifier: usize, logging: Option<Logger>) -> PerOperatorState<T> {

        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let (gis_summary, shared_progress) = scope.get_internal_summary();

        assert_eq!(gis_summary.len(), inputs);
        assert!(!gis_summary.iter().any(|x| x.len() != outputs));

        PerOperatorState {
            name:               scope.name().to_owned(),
            operator:           Some(scope),
            index,
            id:                 identifier,
            local,
            inputs,
            outputs,
            edges:              vec![vec![]; outputs],

            recently_active:    true,
            notify,

            logging,

            shared_progress,
            gis_summary,
        }
    }

    pub fn schedule(
        &mut self,
        _outstanding_messages: &[MutableAntichain<T>],  // the reported outstanding messages to the operator.
        internal_capabilities: &[MutableAntichain<T>],  // the reported internal capabilities of the operator.
        external_frontier: &[MutableAntichain<T>],      // the reported external frontier of the operator.
    ) -> bool {

        let active = if let Some(ref mut operator) = self.operator {

            // At this point we can assemble several signals to determine if we can possibly not schedule
            // the operator. At the moment we take what I hope is a conservative approach in which any of
            // the following will require an operator to be rescheduled:
            //
            //   1. There are any post-filter progress changes to communicate and self.notify is true, or
            //   2. The operator performed progress updates in its last execution or reported activity, or
            //   3. There exist outstanding input messages on any input, or
            //   4. There exist held internal capabilities on any output.
            //
            // The first reason is important because any operator could respond arbitrarily to progress
            // updates, with the most obvious example being the `probe` operator. Not invoking this call
            // on a probe operator can currently spin-block the computation, which is clearly a disaster.
            //
            // The second reason is due to the implicit communication that calling `push_external_progress`
            // indicates a receipt of sent messages. Even with no change in capabilities, if they are empty
            // and the messages are now received this can unblock an operator. Furthermore, if an operator
            // reports that it is active despite the absence of messages and capabilities, then we must
            // reschedule it due to a lack of insight as to whether it can run or not (consider: subgraphs
            // with internal messages or capabilities).
            //
            // The third reason is that the operator could plausibly receive a input data. If there are no
            // outstanding input messages, then while the operator *could* receive input data, the progress
            // information announcing the message's existence hasn't arrived yet, but soon will. It is safe
            // to await this progress information.
            //
            // The fourth reason is that operators holding capabilities can decide to exert or drop them for
            // any reason, perhaps just based on the number of times they have been called. In the absence
            // of any restriction on what would unblock them, we need to continually poll them.
            // NOTE: We could consider changing this to: operators that may unblock arbitrarily must express
            // activity, removing "holds capability" as a reason to schedule an operator. This may be fairly
            // easy to get wrong (for the operator implementor) and we should be careful here.

            let any_progress_updates = self.shared_progress.borrow_mut().frontiers.iter_mut().any(|buffer| !buffer.is_empty()) && self.notify;
            let _was_recently_active = self.recently_active;
            let _outstanding_messages = _outstanding_messages.iter().any(|chain| !chain.is_empty());
            let _held_capabilities = internal_capabilities.iter().any(|chain| !chain.is_empty());

            // TODO: This is reasonable, in principle, but `_outstanding_messages` determined from pointstamps
            //       alone leaves us in a weird state should progress messages get blocked by non-execution of
            //       e.g. the exchange operator in the exchange.rs example.

            if any_progress_updates || _was_recently_active || _outstanding_messages || _held_capabilities
            {

                use ::logging::{PushProgressEvent, ScheduleEvent, StartStop};

                let self_id = self.id;  // avoid capturing `self` in logging closures.

                if any_progress_updates {
                    self.logging.as_mut().map(|l|
                        l.log(PushProgressEvent { op_id: self_id });
                    });
                }

                self.logging.as_mut().map(|l| l.log(ScheduleEvent {
                    id: self_id, start_stop: StartStop::Start
                }));

                let internal_activity = operator.schedule();

                let shared_progress = &mut *self.shared_progress.borrow_mut();
                let did_work =
                    shared_progress.consumeds.iter_mut().any(|x| !x.is_empty()) ||
                    shared_progress.internals.iter_mut().any(|x| !x.is_empty()) ||
                    shared_progress.produceds.iter_mut().any(|x| !x.is_empty());

                // // The operator was recently active if it did anything, or reports activity.
                self.recently_active = did_work || internal_activity;

                self.logging.as_mut().map(|l|
                    l.log(ScheduleEvent {
                        id: self_id,
                        start_stop: StartStop::Stop { activity: did_work }
                    }));

                internal_activity
            }
            else {
                false
            }
        }
        else {

            // If the operator is closed and we are reporting progress at it, something has surely gone wrong.
            if self.shared_progress.borrow_mut().frontiers.iter_mut().any(|x| !x.is_empty()) {
                println!("Operator prematurely shut down: {}", self.name);
                println!("  {:?}", self.notify);
                println!("  {:?}", self.shared_progress.borrow_mut().frontiers);
                panic!();
            }

            // A closed operator shouldn't keep anything open.
            false
        };

        // DEBUG: test validity of updates.
        // TODO: This test is overly pessimistic for current implementations, which may report they acquire
        // capabilities based on the receipt of messages they cannot express (e.g. messages internal to a
        // subgraph). This suggests a weakness in the progress tracking protocol, that it is hard to validate
        // locally, which we could aim to improve.
        // #[cfg(debug_assertions)]
        // {
        //     // 1. each increment to self.internal_buffer needs to correspond to a positive self.consumed_buffer
        //     for index in 0 .. self.internal_buffer.len() {
        //         for change in self.internal_buffer[index].iter() {
        //             if change.1 > 0 {
        //                 let consumed = self.consumed_buffer.iter_mut().any(|x| x.iter().any(|y| y.1 > 0 && y.0.less_equal(&change.0)));
        //                 let internal = self.internal_capabilities[index].less_equal(&change.0);
        //                 if !consumed && !internal {
        //                     panic!("Progress error; internal {:?}: {:?}", self.name, change);
        //                 }
        //             }
        //         }
        //     }
        //     // 2. each produced message must correspond to a held capability or consumed message
        //     for index in 0 .. self.produced_buffer.len() {
        //         for change in self.produced_buffer[index].iter() {
        //             if change.1 > 0 {
        //                 let consumed = self.consumed_buffer.iter_mut().any(|x| x.iter().any(|y| y.1 > 0 && y.0.less_equal(&change.0)));
        //                 let internal = self.internal_capabilities[index].less_equal(&change.0);
        //                 if !consumed && !internal {
        //                     panic!("Progress error; produced {:?}: {:?}", self.name, change);
        //                 }
        //             }
        //         }
        //     }
        // }
        // DEBUG: end validity test.

        // We can shut down the operator if several conditions are met.
        //
        // We look for operators that (i) still exist, (ii) report no activity, (iii) will no longer
        // receive incoming messages, and (iv) hold no capabilities.
        if self.operator.is_some() &&
           !active &&
           self.notify && external_frontier.iter().all(|x| x.is_empty()) &&
           internal_capabilities.iter().all(|x| x.is_empty()) {
               self.operator = None;
               self.name = format!("{}(tombstone)", self.name);
           }

        active
    }

    fn extract_progress(&mut self, pointstamps: &mut ChangeBatch<(Location, T)>) {

        let shared_progress = &mut *self.shared_progress.borrow_mut();

        // Migrate consumeds, internals, produceds into progress statements.
        for (input, consumed) in shared_progress.consumeds.iter_mut().enumerate() {
            let target = Location::new_target(self.index, input);
            for (time, delta) in consumed.drain() {
                pointstamps.update((target, time), -delta);
            }
        }
        for (output, internal) in shared_progress.internals.iter_mut().enumerate() {
            let source = Location::new_source(self.index, output);
            for (time, delta) in internal.drain() {
                pointstamps.update((source, time.clone()), delta);
            }
        }
        for (output, produced) in shared_progress.produceds.iter_mut().enumerate() {
            for (time, delta) in produced.drain() {
                for target in &self.edges[output] {
                    pointstamps.update((Location::from(*target), time.clone()), delta);
                }
            }
        }
    }
}
