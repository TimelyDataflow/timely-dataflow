//! A dataflow subgraph
//!
//! Timely dataflow graphs can be nested hierarchically, where some region of
//! graph is grouped, and presents upwards as an operator. This grouping needs
//! some care, to make sure that the presented operator reflects the behavior
//! of the grouped operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{BinaryHeap, HashMap};
use std::cmp::Reverse;

use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelySummaryLogger as SummaryLogger;

use crate::scheduling::Schedule;
use crate::scheduling::activate::Activations;

use crate::progress::frontier::{MutableAntichain, MutableAntichainFilter};
use crate::progress::{Timestamp, Operate, operate::SharedProgress};
use crate::progress::{Location, Port, Source, Target};
use crate::progress::operate::{FrontierInterest, Connectivity, PortConnectivity};
use crate::progress::ChangeBatch;
use crate::progress::broadcast::Progcaster;
use crate::progress::reachability;
use crate::progress::timestamp::Refines;

use crate::worker::ProgressMode;

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
    pub path: Rc<[usize]>,

    /// The index assigned to the subgraph by its parent.
    index: usize,

    /// A global identifier for this subgraph.
    identifier: usize,

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
    /// Typed logging handle for operator summaries.
    summary_logging: Option<SummaryLogger<TInner::Summary>>,
}

impl<TOuter, TInner> SubgraphBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    /// Allocates a new input to the subgraph and returns the target to that input in the outer graph.
    pub fn new_input(&mut self, shared_counts: Rc<RefCell<ChangeBatch<TInner>>>) -> Target {
        self.input_messages.push(shared_counts);
        Target::new(self.index, self.input_messages.len() - 1)
    }

    /// Allocates a new output from the subgraph and returns the source of that output in the outer graph.
    pub fn new_output(&mut self) -> Source {
        self.output_capabilities.push(MutableAntichain::new());
        Source::new(self.index, self.output_capabilities.len() - 1)
    }

    /// Introduces a dependence from the source to the target.
    ///
    /// This method does not effect data movement, but rather reveals to the progress tracking infrastructure
    /// that messages produced by `source` should be expected to be consumed at `target`.
    pub fn connect(&mut self, source: Source, target: Target) {
        self.edge_stash.push((source, target));
    }

    /// Creates a `SubgraphBuilder` from a path of indexes from the dataflow root to the subgraph,
    /// terminating with the local index of the new subgraph itself.
    pub fn new_from(
        path: Rc<[usize]>,
        identifier: usize,
        logging: Option<Logger>,
        summary_logging: Option<SummaryLogger<TInner::Summary>>,
        name: &str,
    )
        -> SubgraphBuilder<TOuter, TInner>
    {
        // Put an empty placeholder for "outer scope" representative.
        let children = vec![PerOperatorState::empty(0, 0)];
        let index = path[path.len() - 1];

        SubgraphBuilder {
            name: name.to_owned(),
            path,
            index,
            identifier,
            children,
            child_count: 1,
            edge_stash: Vec::new(),
            input_messages: Vec::new(),
            output_capabilities: Vec::new(),
            logging,
            summary_logging,
        }
    }

    /// Allocates a new child identifier, for later use.
    pub fn allocate_child_id(&mut self) -> usize {
        self.child_count += 1;
        self.child_count - 1
    }

    /// Adds a new child to the subgraph.
    pub fn add_child(&mut self, child: Box<dyn Operate<TInner>>, index: usize, identifier: usize) {
        let child = PerOperatorState::new(child, index, identifier, self.logging.clone(), &mut self.summary_logging);
        if let Some(l) = &mut self.logging {
            let mut child_path = Vec::with_capacity(self.path.len() + 1);
            child_path.extend_from_slice(&self.path[..]);
            child_path.push(index);

            l.log(crate::logging::OperatesEvent {
                id: identifier,
                addr: child_path,
                name: child.name.to_owned(),
            });
        }
        self.children.push(child);
    }

    /// Now that initialization is complete, actually build a subgraph.
    pub fn build<A: crate::worker::AsWorker>(mut self, worker: &mut A) -> Subgraph<TOuter, TInner> {
        // at this point, the subgraph is frozen. we should initialize any internal state which
        // may have been determined after construction (e.g. the numbers of inputs and outputs).
        // we also need to determine what to return as a summary and initial capabilities, which
        // will depend on child summaries and capabilities, as well as edges in the subgraph.

        // perhaps first check that the children are sanely identified
        self.children.sort_unstable_by(|x,y| x.index.cmp(&y.index));
        assert!(self.children.iter().enumerate().all(|(i,x)| i == x.index));

        let inputs = self.input_messages.len();
        let outputs = self.output_capabilities.len();

        // Create empty child zero representative.
        self.children[0] = PerOperatorState::empty(outputs, inputs);

        // Pipeline group fusion: detect and fuse groups of pipeline-connected operators.
        let fuse_chain_length = worker.config().fuse_chain_length;
        if fuse_chain_length >= 2 {
            let groups = detect_groups(&self.children, &self.edge_stash, fuse_chain_length);
            for group in groups {
                fuse_group::<TInner>(&mut self.children, &mut self.edge_stash, &group);
            }
        }

        let mut builder = reachability::Builder::new();

        // Child 0 has `inputs` outputs and `outputs` inputs, not yet connected.
        let summary = (0..outputs).map(|_| PortConnectivity::default()).collect();
        builder.add_node(0, outputs, inputs, summary);
        for (index, child) in self.children.iter().enumerate().skip(1) {
            // Tombstoned children are added with (0, 0) inputs/outputs and empty summary
            // to preserve index positions in the reachability tracker.
            builder.add_node(index, child.inputs, child.outputs, child.internal_summary.clone());
        }

        for (source, target) in self.edge_stash {
            self.children[source.node].edges[source.port].push(target);
            builder.add_edge(source, target);
        }

        // The `None` argument is optional logging infrastructure.
        let type_name = std::any::type_name::<TInner>();
        let reachability_logging =
        worker.logger_for(&format!("timely/reachability/{type_name}"))
              .map(|logger| reachability::logging::TrackerLogger::new(self.identifier, logger));
        let progress_logging = worker.logger_for(&format!("timely/progress/{type_name}"));
        let (tracker, scope_summary) = builder.build(reachability_logging);

        let progcaster = Progcaster::new(worker, Rc::clone(&self.path), self.identifier, self.logging.clone(), progress_logging);

        let mut incomplete = vec![true; self.children.len()];
        incomplete[0] = false;
        // Tombstoned children are not incomplete.
        for (i, child) in self.children.iter().enumerate().skip(1) {
            if child.inputs == 0 && child.outputs == 0 && child.operator.is_none() {
                incomplete[i] = false;
            }
        }
        let incomplete_count = incomplete.iter().filter(|&&b| b).count();

        let activations = worker.activations();

        activations.borrow_mut().activate(&self.path[..]);

        // The subgraph's per-input interest is conservatively the max across all children's inputs.
        let max_interest = self.children.iter()
            .flat_map(|c| c.notify.iter().copied())
            .max()
            .unwrap_or(FrontierInterest::Never);
        let notify_me: Vec<FrontierInterest> = vec![max_interest; inputs];

        Subgraph {
            name: self.name,
            path: self.path,
            inputs,
            outputs,
            incomplete,
            incomplete_count,
            activations,
            temp_active: BinaryHeap::new(),
            maybe_shutdown: Vec::new(),
            children: self.children,
            input_messages: self.input_messages,
            output_capabilities: self.output_capabilities,

            local_pointstamp: ChangeBatch::new(),
            final_pointstamp: ChangeBatch::new(),
            progcaster,
            pointstamp_tracker: tracker,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),
            scope_summary,

            progress_mode: worker.config().progress_mode,
            notify_me,
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
    name: String,           // an informative name.
    /// Path of identifiers from the root.
    pub path: Rc<[usize]>,
    inputs: usize,          // number of inputs.
    outputs: usize,         // number of outputs.

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<TInner>>,

    incomplete: Vec<bool>,   // the incompletion status of each child.
    incomplete_count: usize, // the number of incomplete children.

    // shared activations (including children).
    activations: Rc<RefCell<Activations>>,
    temp_active: BinaryHeap<Reverse<usize>>,
    maybe_shutdown: Vec<usize>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<ChangeBatch<TInner>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    // pointstamp messages to exchange. ultimately destined for `messages` or `internal`.
    local_pointstamp: ChangeBatch<(Location, TInner)>,
    final_pointstamp: ChangeBatch<(Location, TInner)>,

    // Graph structure and pointstamp tracker.
    // pointstamp_builder: reachability::Builder<TInner>,
    pointstamp_tracker: reachability::Tracker<TInner>,

    // channel / whatever used to communicate pointstamp updates to peers.
    progcaster: Progcaster<TInner>,

    shared_progress: Rc<RefCell<SharedProgress<TOuter>>>,
    scope_summary: Connectivity<TInner::Summary>,

    progress_mode: ProgressMode,

    notify_me: Vec<FrontierInterest>,
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

        self.accept_frontier();         // Accept supplied frontier changes.
        self.harvest_inputs();          // Count records entering the scope.

        // Receive post-exchange progress updates.
        self.progcaster.recv(&mut self.final_pointstamp);

        // Commit and propagate final pointstamps.
        self.propagate_pointstamps();

        {   // Enqueue active children; scoped to let borrow drop.
            let temp_active = &mut self.temp_active;
            self.activations
                .borrow_mut()
                .for_extensions(&self.path[..], |index| temp_active.push(Reverse(index)));
        }

        // Schedule child operators.
        //
        // We should be able to schedule arbitrary subsets of children, as
        // long as we eventually schedule all children that need to do work.
        let mut scheduled = std::collections::HashSet::new();
        scheduled.insert(0);  // Child 0 is the subgraph boundary, never scheduled.
        while let Some(Reverse(index)) = self.temp_active.pop() {
            if !scheduled.insert(index) { continue; }
            // Tombstoned group members forward activations to their representative.
            if let Some(fwd) = self.children[index].forward_to {
                self.temp_active.push(Reverse(fwd));
            } else {
                // TODO: This is a moment where a scheduling decision happens.
                self.activate_child(index);
            }
        }

        // Transmit produced progress updates.
        self.send_progress();

        // If child scopes surface more final pointstamp updates we must re-execute.
        if !self.final_pointstamp.is_empty() {
            self.activations.borrow_mut().activate(&self.path[..]);
        }

        // A subgraph is incomplete if any child is incomplete, or there are outstanding messages.
        let incomplete = self.incomplete_count > 0;
        let tracking = self.pointstamp_tracker.tracking_anything();

        incomplete || tracking
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

        let child = &mut self.children[child_index];

        let incomplete = child.schedule();

        if incomplete != self.incomplete[child_index] {
            if incomplete { self.incomplete_count += 1; }
            else          { self.incomplete_count -= 1; }
            self.incomplete[child_index] = incomplete;
        }

        if !incomplete {
            // Consider shutting down the child, if neither capabilities nor input frontier.
            let child_state = self.pointstamp_tracker.node_state(child_index);
            let frontiers_empty = child_state.targets.iter().all(|x| x.implications.is_empty());
            let no_capabilities = child_state.sources.iter().all(|x| x.pointstamps.is_empty());
            if frontiers_empty && no_capabilities {
                child.shut_down();
            }
        }
        else {
            // In debug mode, check that the progress statements do not violate invariants.
            #[cfg(debug_assertions)] {
                child.validate_progress(self.pointstamp_tracker.node_state(child_index));
            }
        }

        // Extract progress statements into either pre- or post-exchange buffers.
        if child.local {
            child.extract_progress(&mut self.local_pointstamp, &mut self.temp_active);
        }
        else {
            child.extract_progress(&mut self.final_pointstamp, &mut self.temp_active);
        }

        incomplete
    }

    /// Move frontier changes from parent into progress statements.
    fn accept_frontier(&mut self) {
        for (port, changes) in self.shared_progress.borrow_mut().frontiers.iter_mut().enumerate() {
            let source = Source::new(0, port);
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
    /// be performed atomically, before control is returned. These are:
    ///
    /// 1. Changes to child zero's outputs are reported as consumed messages.
    /// 2. Changes to child zero's inputs are reported as produced messages.
    /// 3. Frontiers for child zero's inputs are reported as internal capabilities.
    ///
    /// Perhaps importantly, the frontiers for child zero are determined *without*
    /// the messages that are produced for child zero inputs, as we only want to
    /// report retained internal capabilities, and not now-external messages.
    ///
    /// In the course of propagating progress changes, we also propagate progress
    /// changes for all of the managed child operators.
    fn propagate_pointstamps(&mut self) {

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
        let (pushed, operators) = self.pointstamp_tracker.pushed();
        for ((location, time), diff) in pushed.drain() {
            self.maybe_shutdown.push(location.node);
            // Targets are actionable, sources are not.
            if let crate::progress::Port::Target(port) = location.port {
                // Activate based on expressed frontier interest for this input.
                let activate = match self.children[location.node].notify[port] {
                    FrontierInterest::Always => true,
                    FrontierInterest::IfCapability => { operators[location.node].cap_counts > 0 }
                    FrontierInterest::Never => false,
                };
                if activate { self.temp_active.push(Reverse(location.node)); }

                // Keep this current independent of the interest.
                self.children[location.node]
                    .shared_progress
                    .borrow_mut()
                    .frontiers[port]
                    .update(time, diff);
            }
        }

        // Consider scheduling each recipient of progress information to shut down.
        self.maybe_shutdown.sort_unstable();
        self.maybe_shutdown.dedup();
        for child_index in self.maybe_shutdown.drain(..) {
            let child_state = self.pointstamp_tracker.node_state(child_index);
            let frontiers_empty = child_state.targets.iter().all(|x| x.implications.is_empty());
            let no_capabilities = child_state.cap_counts == 0;
            if frontiers_empty && no_capabilities {
                self.temp_active.push(Reverse(child_index));
            }
        }

        // Extract child zero frontier changes and report as internal capability changes.
        for (output, internal) in self.shared_progress.borrow_mut().internals.iter_mut().enumerate() {
            self.pointstamp_tracker
                .pushed_output()[output]
                .drain()
                .map(|(time, diff)| (time.to_outer(), diff))
                .filter_through(&mut self.output_capabilities[output])
                .for_each(|(time, diff)| internal.update(time, diff));
        }
    }

    /// Sends local progress updates to all workers.
    ///
    /// This method does not guarantee that all of `self.local_pointstamps` are
    /// sent, but that no blocking pointstamps remain
    fn send_progress(&mut self) {

        // If we are requested to eagerly send progress updates, or if there are
        // updates visible in the scope-wide frontier, we must send all updates.
        let must_send = self.progress_mode == ProgressMode::Eager || {
            let tracker = &mut self.pointstamp_tracker;
            self.local_pointstamp
                .iter()
                .any(|((location, time), diff)|
                    // Must publish scope-wide visible subtractions.
                    tracker.is_global(*location, time) && *diff < 0 ||
                    // Must confirm the receipt of inbound messages.
                    location.node == 0
                )
        };

        if must_send {
            self.progcaster.send(&mut self.local_pointstamp);
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
    fn initialize(mut self: Box<Self>) -> (Connectivity<TOuter::Summary>, Rc<RefCell<SharedProgress<TOuter>>>, Box<dyn Schedule>) {

        // double-check that child 0 (the outside world) is correctly shaped.
        assert_eq!(self.children[0].outputs, self.inputs());
        assert_eq!(self.children[0].inputs, self.outputs());

        // Note that we need to have `self.inputs()` elements in the summary
        // with each element containing `self.outputs()` antichains regardless
        // of how long `self.scope_summary` is
        let mut internal_summary = vec![PortConnectivity::default(); self.inputs()];
        for (input_idx, input) in self.scope_summary.iter().enumerate() {
            for (output_idx, output) in input.iter_ports() {
                for outer in output.elements().iter().cloned().map(TInner::summarize) {
                    internal_summary[input_idx].insert(output_idx, outer);
                }
            }
        }

        debug_assert_eq!(
            internal_summary.len(),
            self.inputs(),
            "the internal summary should have as many elements as there are inputs",
        );
        debug_assert!(
            internal_summary.iter().all(|os| os.iter_ports().all(|(o,_)| o < self.outputs())),
            "each element of the internal summary should only reference valid outputs",
        );

        // Each child has expressed initial capabilities (their `shared_progress.internals`).
        // We introduce these into the progress tracker to determine the scope's initial
        // internal capabilities.
        for child in self.children.iter_mut() {
            child.extract_progress(&mut self.final_pointstamp, &mut self.temp_active);
        }

        self.propagate_pointstamps();  // Propagate expressed capabilities to output frontiers.

        // Return summaries and shared progress information.
        (internal_summary, Rc::clone(&self.shared_progress), self)
    }

    fn notify_me(&self) -> &[FrontierInterest] { &self.notify_me }
}

struct PerOperatorState<T: Timestamp> {

    name: String,       // name of the operator
    index: usize,       // index of the operator within its parent scope
    id: usize,          // worker-unique identifier

    local: bool,        // indicates whether progress information is pre-circulated or not
    notify: Vec<FrontierInterest>,
    pipeline: bool,     // indicates whether all inputs use thread-local (pipeline) channels
    inputs: usize,      // number of inputs to the operator
    outputs: usize,     // number of outputs from the operator

    operator: Option<Box<dyn Schedule>>,

    edges: Vec<Vec<Target>>,    // edges from the outputs of the operator

    shared_progress: Rc<RefCell<SharedProgress<T>>>,

    internal_summary: Connectivity<T::Summary>,   // cached result from initialize.

    logging: Option<Logger>,

    /// For tombstoned group members: forward activations to the group representative.
    forward_to: Option<usize>,
}

impl<T: Timestamp> PerOperatorState<T> {

    fn empty(inputs: usize, outputs: usize) -> PerOperatorState<T> {
        PerOperatorState {
            name:       "External".to_owned(),
            operator:   None,
            index:      0,
            id:         usize::MAX,
            local:      false,
            notify:     vec![FrontierInterest::IfCapability; inputs],
            pipeline:   false,
            inputs,
            outputs,

            edges: vec![Vec::new(); outputs],

            logging: None,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs,outputs))),
            internal_summary: Vec::new(),
            forward_to: None,
        }
    }

    pub fn new(
        scope: Box<dyn Operate<T>>,
        index: usize,
        identifier: usize,
        logging: Option<Logger>,
        summary_logging: &mut Option<SummaryLogger<T::Summary>>,
    ) -> PerOperatorState<T>
    {
        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me().to_vec();
        let pipeline = scope.pipeline();

        let (internal_summary, shared_progress, operator) = scope.initialize();

        if let Some(l) = summary_logging {
            l.log(crate::logging::OperatesSummaryEvent {
                id: identifier,
                summary: internal_summary.clone(),
            })
        }

        assert_eq!(
            internal_summary.len(),
            inputs,
            "operator summary has {} inputs when {} were expected",
            internal_summary.len(),
            inputs,
        );
        assert!(
            internal_summary.iter().all(|os| os.iter_ports().all(|(o,_)| o < outputs)),
            "operator summary references invalid output port",
        );

        PerOperatorState {
            name:               operator.name().to_owned(),
            operator:           Some(operator),
            index,
            id:                 identifier,
            local,
            notify,
            pipeline,
            inputs,
            outputs,
            edges:              vec![vec![]; outputs],

            logging,

            shared_progress,
            internal_summary,
            forward_to: None,
        }
    }

    pub fn schedule(&mut self) -> bool {

        if let Some(ref mut operator) = self.operator {

            // Perhaps log information about the start of the schedule call.
            if let Some(l) = self.logging.as_mut() {
                // FIXME: There is no contract that the operator must consume frontier changes.
                //        This report could be spurious.
                // TODO:  Perhaps fold this in to `ScheduleEvent::start()` as a "reason"?
                let frontiers = &mut self.shared_progress.borrow_mut().frontiers[..];
                if frontiers.iter_mut().any(|buffer| !buffer.is_empty()) {
                    l.log(crate::logging::PushProgressEvent { op_id: self.id })
                }

                l.log(crate::logging::ScheduleEvent::start(self.id));
            }

            let incomplete = operator.schedule();

            // Perhaps log information about the stop of the schedule call.
            if let Some(l) = self.logging.as_mut() {
                l.log(crate::logging::ScheduleEvent::stop(self.id));
            }

            incomplete
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
        }
    }

    fn shut_down(&mut self) {
        if self.operator.is_some() {
            if let Some(l) = self.logging.as_mut() {
                l.log(crate::logging::ShutdownEvent{ id: self.id });
            }
            self.operator = None;
        }
    }

    /// Extracts shared progress information and converts to pointstamp changes.
    fn extract_progress(&self, pointstamps: &mut ChangeBatch<(Location, T)>, temp_active: &mut BinaryHeap<Reverse<usize>>) {

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
                    temp_active.push(Reverse(target.node));
                }
            }
        }
    }

    /// Test the validity of `self.shared_progress`.
    ///
    /// The validity of shared progress information depends on both the external frontiers and the
    /// internal capabilities, as events can occur that cannot be explained locally otherwise.
    #[allow(dead_code)]
    fn validate_progress(&self, child_state: &reachability::PerOperator<T>) {

        let shared_progress = &mut *self.shared_progress.borrow_mut();

        // Increments to internal capabilities require a consumed input message, a
        for (output, internal) in shared_progress.internals.iter_mut().enumerate() {
            for (time, diff) in internal.iter() {
                if *diff > 0 {
                    let consumed = shared_progress.consumeds.iter_mut().any(|x| x.iter().any(|(t,d)| *d > 0 && t.less_equal(time)));
                    let internal = child_state.sources[output].implications.less_equal(time);
                    if !consumed && !internal {
                        println!("Increment at {:?}, not supported by\n\tconsumed: {:?}\n\tinternal: {:?}", time, shared_progress.consumeds, child_state.sources[output].implications);
                        panic!("Progress error; internal {:?}", self.name);
                    }
                }
            }
        }
        for (output, produced) in shared_progress.produceds.iter_mut().enumerate() {
            for (time, diff) in produced.iter() {
                if *diff > 0 {
                    let consumed = shared_progress.consumeds.iter_mut().any(|x| x.iter().any(|(t,d)| *d > 0 && t.less_equal(time)));
                    let internal = child_state.sources[output].implications.less_equal(time);
                    if !consumed && !internal {
                        println!("Increment at {:?}, not supported by\n\tconsumed: {:?}\n\tinternal: {:?}", time, shared_progress.consumeds, child_state.sources[output].implications);
                        panic!("Progress error; produced {:?}", self.name);
                    }
                }
            }
        }
    }
}

// Explicitly shut down the operator to get logged information.
impl<T: Timestamp> Drop for PerOperatorState<T> {
    fn drop(&mut self) {
        self.shut_down();
    }
}

// --- Pipeline group fusion ---

/// Returns true if an operator has identity internal summaries on all (input, output) pairs.
/// That is, every connected (input, output) pair has summary `Antichain::from_elem(Default::default())`.
fn has_identity_summary<T: Timestamp>(child: &PerOperatorState<T>) -> bool {
    for input_pc in child.internal_summary.iter() {
        for (_port, ac) in input_pc.iter_ports() {
            if ac.len() != 1 || ac.elements()[0] != Default::default() {
                return false;
            }
        }
    }
    // Must have at least one connection (empty summary means no paths).
    child.internal_summary.iter().any(|pc| pc.iter_ports().next().is_some())
}

/// Returns true if an operator is eligible for group fusion.
fn is_fusible<T: Timestamp>(child: &PerOperatorState<T>) -> bool {
    child.operator.is_some()
        && !child.notify
        && has_identity_summary(child)
}

/// Detects fusible groups of operators connected by pipeline edges.
///
/// Uses union-find to group operators connected by fusible edges into components.
/// An edge is fusible when both endpoints are fusible operators and the target
/// uses pipeline (thread-local) channels. No fan-in/fan-out or 1-in/1-out restriction.
///
/// Returns groups of at least `min_length` operators, identified by child index.
fn detect_groups<T: Timestamp>(
    children: &[PerOperatorState<T>],
    edge_stash: &[(Source, Target)],
    min_length: usize,
) -> Vec<Vec<usize>> {
    // Mark fusible operators.
    let fusible: Vec<bool> = children.iter().enumerate().map(|(i, child)| {
        i != 0 && is_fusible(child)
    }).collect();

    // Union-Find structure.
    let n = children.len();
    let mut parent: Vec<usize> = (0..n).collect();
    let mut rank: Vec<usize> = vec![0; n];

    fn find(parent: &mut [usize], x: usize) -> usize {
        if parent[x] != x {
            parent[x] = find(parent, parent[x]);
        }
        parent[x]
    }

    fn union(parent: &mut [usize], rank: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra == rb { return; }
        if rank[ra] < rank[rb] {
            parent[ra] = rb;
        } else if rank[ra] > rank[rb] {
            parent[rb] = ra;
        } else {
            parent[rb] = ra;
            rank[ra] += 1;
        }
    }

    // For each edge, if both endpoints are fusible and target uses pipeline pact, union them.
    for (source, target) in edge_stash.iter() {
        let src = source.node;
        let tgt = target.node;
        if src == 0 || tgt == 0 { continue; }
        if !fusible[src] || !fusible[tgt] { continue; }
        if !children[tgt].pipeline { continue; }
        union(&mut parent, &mut rank, src, tgt);
    }

    // Collect components.
    let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 1..n {
        if fusible[i] {
            let root = find(&mut parent, i);
            components.entry(root).or_default().push(i);
        }
    }

    // Filter by minimum size and sort members for determinism.
    components.into_values()
        .filter(|group| group.len() >= min_length)
        .map(|mut group| { group.sort(); group })
        .collect()
}

/// Topological sort of group members using Kahn's algorithm on internal edges.
fn topological_sort(
    members: &[usize],
    edge_stash: &[(Source, Target)],
) -> Vec<usize> {
    let member_set: std::collections::HashSet<usize> = members.iter().cloned().collect();
    let member_to_pos: HashMap<usize, usize> = members.iter().enumerate().map(|(i, &m)| (m, i)).collect();
    let n = members.len();

    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (source, target) in edge_stash.iter() {
        if member_set.contains(&source.node) && member_set.contains(&target.node) {
            let from = member_to_pos[&source.node];
            let to = member_to_pos[&target.node];
            // Avoid counting duplicate edges for the same (from, to) pair multiple times
            // for in-degree. We track adjacency; Kahn's handles it correctly.
            adj[from].push(to);
            in_degree[to] += 1;
        }
    }

    let mut queue: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    for i in 0..n {
        if in_degree[i] == 0 {
            queue.push_back(i);
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(pos) = queue.pop_front() {
        order.push(members[pos]);
        for &next in &adj[pos] {
            in_degree[next] -= 1;
            if in_degree[next] == 0 {
                queue.push_back(next);
            }
        }
    }

    assert_eq!(order.len(), n, "group contains a cycle, which should be impossible with identity summaries");
    order
}

/// A member of a fused group, holding the original operator and its progress handle.
struct GroupMember<T: Timestamp> {
    operator: Box<dyn Schedule>,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
}

/// Schedules a DAG of pipeline-connected operators as a single unit.
///
/// The group presents as a single operator to the subgraph with `input_map.len()` inputs
/// and `output_map.len()` outputs. Members are scheduled in topological order.
/// Intermediate progress is hidden from the reachability tracker.
struct GroupScheduler<T: Timestamp> {
    name: String,
    path: Vec<usize>,
    /// Progress visible to the subgraph.
    group_progress: Rc<RefCell<SharedProgress<T>>>,
    /// Operators in topological order, with their individual SharedProgress handles.
    members: Vec<GroupMember<T>>,
    /// Group input i -> (member index in members vec, member input port)
    input_map: Vec<(usize, usize)>,
    /// Group output j -> (member index in members vec, member output port)
    output_map: Vec<(usize, usize)>,
    /// capability_map[member_idx][output_port] -> list of group output indices
    capability_map: Vec<Vec<Vec<usize>>>,
}

impl<T: Timestamp> Schedule for GroupScheduler<T> {
    fn name(&self) -> &str { &self.name }
    fn path(&self) -> &[usize] { &self.path }

    fn schedule(&mut self) -> bool {
        let n = self.members.len();
        assert!(n > 0);

        // Step 1: Forward group's input frontier changes to the appropriate members.
        {
            let mut group_sp = self.group_progress.borrow_mut();
            for (i, &(member_idx, member_port)) in self.input_map.iter().enumerate() {
                let mut member_sp = self.members[member_idx].shared_progress.borrow_mut();
                for (time, diff) in group_sp.frontiers[i].iter() {
                    member_sp.frontiers[member_port].update(time.clone(), *diff);
                }
            }
        }

        // Step 2: Schedule each member in topological order.
        let mut any_incomplete = false;
        for i in 0..n {
            let incomplete = self.members[i].operator.schedule();
            any_incomplete = any_incomplete || incomplete;
        }

        // Step 3: Aggregate progress.
        {
            let mut group_sp = self.group_progress.borrow_mut();

            // consumeds: for each group input, take from the corresponding member.
            for (i, &(member_idx, member_port)) in self.input_map.iter().enumerate() {
                let mut member_sp = self.members[member_idx].shared_progress.borrow_mut();
                for (time, diff) in member_sp.consumeds[member_port].iter() {
                    group_sp.consumeds[i].update(time.clone(), *diff);
                }
            }

            // produceds: for each group output, take from the corresponding member.
            for (j, &(member_idx, member_port)) in self.output_map.iter().enumerate() {
                let mut member_sp = self.members[member_idx].shared_progress.borrow_mut();
                for (time, diff) in member_sp.produceds[member_port].iter() {
                    group_sp.produceds[j].update(time.clone(), *diff);
                }
            }

            // internals: for each member's output port, report at mapped group outputs.
            for (m, member) in self.members.iter().enumerate() {
                let mut member_sp = member.shared_progress.borrow_mut();
                for (port, internal) in member_sp.internals.iter_mut().enumerate() {
                    for (time, diff) in internal.iter() {
                        for &group_out in &self.capability_map[m][port] {
                            group_sp.internals[group_out].update(time.clone(), *diff);
                        }
                    }
                }
            }
        }

        // Step 4: Clear all members' SharedProgress to prevent accumulation.
        for member in self.members.iter() {
            let mut sp = member.shared_progress.borrow_mut();
            for batch in sp.frontiers.iter_mut() { batch.clear(); }
            for batch in sp.consumeds.iter_mut() { batch.clear(); }
            for batch in sp.internals.iter_mut() { batch.clear(); }
            for batch in sp.produceds.iter_mut() { batch.clear(); }
        }

        // Clear the group's own frontiers (consumed by step 1).
        {
            let mut group_sp = self.group_progress.borrow_mut();
            for batch in group_sp.frontiers.iter_mut() { batch.clear(); }
        }

        any_incomplete
    }
}

/// Computes reachability for all (member, output_port) pairs in a single reverse-topological pass.
///
/// Returns `capability_map[topo_pos][output_port] -> sorted Vec<group_output_index>`.
/// Since all summaries are identity, timestamps don't change along any path.
fn compute_all_reachability(
    topo_order: &[usize],
    children: &[PerOperatorState<impl Timestamp>],
    internal_edges: &HashMap<(usize, usize), Vec<(usize, usize)>>,
    member_summaries: &HashMap<usize, Vec<(usize, usize)>>,
    output_port_to_group_output: &HashMap<(usize, usize), Vec<usize>>,
) -> Vec<Vec<Vec<usize>>> {
    let n = topo_order.len();

    // Build reverse lookup: node -> topo_pos.
    let node_to_topo: HashMap<usize, usize> = topo_order.iter().enumerate()
        .map(|(i, &node)| (node, i))
        .collect();

    // reachable[(node, output_port)] -> set of group output indices
    // Use a flat Vec indexed by (topo_pos, port) for fast access.
    // First, compute a port offset table.
    let mut port_offset = Vec::with_capacity(n);
    let mut total_ports = 0usize;
    for &node in topo_order.iter() {
        port_offset.push(total_ports);
        total_ports += children[node].outputs;
    }

    // Each entry is a sorted Vec<usize> of reachable group outputs.
    let mut reachable: Vec<Vec<usize>> = vec![Vec::new(); total_ports];

    // Seed: output ports that are directly group outputs.
    for (&(node, port), group_outs) in output_port_to_group_output.iter() {
        if let Some(&topo_pos) = node_to_topo.get(&node) {
            let idx = port_offset[topo_pos] + port;
            reachable[idx] = group_outs.clone();
            reachable[idx].sort();
            reachable[idx].dedup();
        }
    }

    // Reverse topological pass: propagate reachability backward through edges.
    for rev_pos in (0..n).rev() {
        let node = topo_order[rev_pos];
        let num_outputs = children[node].outputs;

        // For each output port of this node, follow internal edges forward
        // and union the reachability of the downstream ports.
        for port in 0..num_outputs {
            if let Some(targets) = internal_edges.get(&(node, port)) {
                for &(next_node, next_input_port) in targets {
                    // Use next node's summary to find which output ports are reachable from this input.
                    if let Some(connections) = member_summaries.get(&next_node) {
                        if let Some(&next_topo) = node_to_topo.get(&next_node) {
                            for &(inp, outp) in connections.iter() {
                                if inp == next_input_port {
                                    // Merge reachable[next_topo][outp] into reachable[rev_pos][port].
                                    let src_idx = port_offset[next_topo] + outp;
                                    let dst_idx = port_offset[rev_pos] + port;
                                    if src_idx != dst_idx {
                                        // Clone to avoid double borrow.
                                        let to_add = reachable[src_idx].clone();
                                        let dst = &mut reachable[dst_idx];
                                        dst.extend_from_slice(&to_add);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Deduplicate after merging all edges for this port.
            let idx = port_offset[rev_pos] + port;
            reachable[idx].sort();
            reachable[idx].dedup();
        }
    }

    // Reshape into capability_map[topo_pos][output_port].
    let mut capability_map = Vec::with_capacity(n);
    for (topo_pos, &node) in topo_order.iter().enumerate() {
        let num_outputs = children[node].outputs;
        let mut port_map = Vec::with_capacity(num_outputs);
        for port in 0..num_outputs {
            let idx = port_offset[topo_pos] + port;
            port_map.push(std::mem::take(&mut reachable[idx]));
        }
        capability_map.push(port_map);
    }

    capability_map
}

/// Fuses a detected group into a single operator within `children`, rewriting `edge_stash`.
///
/// The representative (lowest index in group) retains its slot and becomes the fused operator.
/// All other group members become tombstones.
fn fuse_group<T: Timestamp>(
    children: &mut [PerOperatorState<T>],
    edge_stash: &mut Vec<(Source, Target)>,
    group: &[usize],
) {
    assert!(group.len() >= 2);
    let group_set: std::collections::HashSet<usize> = group.iter().cloned().collect();
    let representative = *group.iter().min().unwrap();

    // Step 1: Compute topological order.
    let topo_order = topological_sort(group, edge_stash);
    let node_to_topo: HashMap<usize, usize> = topo_order.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    // Step 2: Compute input_map and output_map by scanning edges.
    // Group inputs: (member_node, input_port) pairs where at least one incoming edge originates outside the group.
    // Group outputs: (member_node, output_port) pairs where at least one outgoing edge targets outside the group,
    //                OR the port has no outgoing edges at all within the edge_stash.
    let mut group_input_set: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut group_output_set: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut has_outgoing: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();

    // Collect all output ports of group members.
    let mut all_output_ports: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    for &node in group.iter() {
        for port in 0..children[node].outputs {
            all_output_ports.insert((node, port));
        }
    }

    // Build internal edges map: (src_node, src_port) -> [(tgt_node, tgt_port)]
    let mut internal_edges: HashMap<(usize, usize), Vec<(usize, usize)>> = HashMap::new();

    for (source, target) in edge_stash.iter() {
        let src_in = group_set.contains(&source.node);
        let tgt_in = group_set.contains(&target.node);

        if src_in {
            has_outgoing.insert((source.node, source.port));
        }

        if src_in && tgt_in {
            // Internal edge
            internal_edges.entry((source.node, source.port))
                .or_default()
                .push((target.node, target.port));
        } else if !src_in && tgt_in {
            // Incoming edge from outside
            group_input_set.insert((target.node, target.port));
        } else if src_in && !tgt_in {
            // Outgoing edge to outside
            group_output_set.insert((source.node, source.port));
        }
    }

    // Output ports with no outgoing edges at all are also group outputs.
    for &(node, port) in &all_output_ports {
        if !has_outgoing.contains(&(node, port)) {
            group_output_set.insert((node, port));
        }
    }

    // Sort and assign indices for determinism.
    let mut input_map: Vec<(usize, usize)> = group_input_set.into_iter()
        .map(|(node, port)| (node_to_topo[&node], port))
        .collect();
    input_map.sort();
    // Convert back: input_map elements are (topo_position, port)

    let mut output_map: Vec<(usize, usize)> = group_output_set.into_iter()
        .map(|(node, port)| (node_to_topo[&node], port))
        .collect();
    output_map.sort();

    // Build reverse lookups.
    // (node, input_port) -> group input index
    let input_port_to_group_input: HashMap<(usize, usize), usize> = input_map.iter().enumerate()
        .map(|(i, &(topo_pos, port))| ((topo_order[topo_pos], port), i))
        .collect();

    // (node, output_port) -> group output indices
    let mut output_port_to_group_output: HashMap<(usize, usize), Vec<usize>> = HashMap::new();
    for (j, &(topo_pos, port)) in output_map.iter().enumerate() {
        output_port_to_group_output.entry((topo_order[topo_pos], port))
            .or_default()
            .push(j);
    }

    // Step 3: Compute member summaries (input_port, output_port) connections for each node.
    let mut member_summaries: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
    for &node in group.iter() {
        let mut connections = Vec::new();
        for (inp_idx, pc) in children[node].internal_summary.iter().enumerate() {
            for (out_port, _ac) in pc.iter_ports() {
                connections.push((inp_idx, out_port));
            }
        }
        member_summaries.insert(node, connections);
    }

    // Step 4: Compute capability_map via reachability (single reverse-topological pass).
    // capability_map[topo_pos][output_port] -> list of group output indices
    let capability_map = compute_all_reachability(
        &topo_order, children, &internal_edges, &member_summaries, &output_port_to_group_output,
    );

    // Step 5: Compute composed summary for the group.
    // For each (group_input_i, group_output_j): if there's a reachability path, set identity summary.
    let num_inputs = input_map.len();
    let num_outputs = output_map.len();

    let mut composed_summary: Connectivity<T::Summary> = Vec::with_capacity(num_inputs);
    for &(topo_pos, port) in input_map.iter() {
        let node = topo_order[topo_pos];
        let mut pc = PortConnectivity::default();

        // Find which group outputs are reachable from this input.
        // Use the node's summary to find output ports reachable from this input port,
        // then use capability_map for those output ports.
        if let Some(connections) = member_summaries.get(&node) {
            for &(inp, outp) in connections.iter() {
                if inp == port {
                    for &group_out in &capability_map[topo_pos][outp] {
                        pc.insert(group_out, Default::default());
                    }
                }
            }
        }
        composed_summary.push(pc);
    }

    // Step 6: Extract members in topological order.
    let mut members = Vec::with_capacity(topo_order.len());
    let mut group_name_parts = Vec::new();
    let mut representative_path = Vec::new();

    for &node in topo_order.iter() {
        let child = &mut children[node];
        let operator = child.operator.take().expect("group member must have an operator");
        let shared_progress = Rc::clone(&child.shared_progress);

        if node == representative {
            representative_path = operator.path().to_vec();
        }
        group_name_parts.push(child.name.clone());

        members.push(GroupMember {
            operator,
            shared_progress,
        });
    }

    let group_name = format!("Group[{}]", group_name_parts.join(", "));

    // Step 7: Create the group's SharedProgress.
    let group_progress = Rc::new(RefCell::new(SharedProgress::new(num_inputs, num_outputs)));

    // Transfer initial internal capabilities from ALL members to group_progress,
    // mapped through capability_map.
    {
        let mut group_sp = group_progress.borrow_mut();
        for (topo_pos, member) in members.iter().enumerate() {
            let mut member_sp = member.shared_progress.borrow_mut();
            for (port, internal) in member_sp.internals.iter_mut().enumerate() {
                for (time, diff) in internal.iter() {
                    for &group_out in &capability_map[topo_pos][port] {
                        group_sp.internals[group_out].update(time.clone(), *diff);
                    }
                }
            }
        }
    }

    // Clear all members' internals to prevent double-counting during initialize().
    for member in members.iter() {
        let mut sp = member.shared_progress.borrow_mut();
        for batch in sp.internals.iter_mut() { batch.clear(); }
    }

    let group_scheduler: Box<dyn Schedule> = Box::new(GroupScheduler {
        name: group_name.clone(),
        path: representative_path,
        group_progress: Rc::clone(&group_progress),
        members,
        input_map: input_map.clone(),
        output_map: output_map.clone(),
        capability_map,
    });

    // Step 8: Install the fused operator at the representative slot.
    // Edges are left empty here; the build method populates them from edge_stash.
    let head = &mut children[representative];
    head.name = group_name;
    head.operator = Some(group_scheduler);
    head.shared_progress = group_progress;
    head.internal_summary = composed_summary;
    head.inputs = num_inputs;
    head.outputs = num_outputs;
    head.edges = vec![Vec::new(); num_outputs];

    // Step 9: Tombstone all other group members, forwarding activations to representative.
    for &node in group.iter() {
        if node == representative { continue; }
        let child = &mut children[node];
        child.name = format!("Tombstone({})", child.name);
        child.operator = None;
        child.shared_progress = Rc::new(RefCell::new(SharedProgress::new(0, 0)));
        child.edges = Vec::new();
        child.inputs = 0;
        child.outputs = 0;
        child.internal_summary = Vec::new();
        child.forward_to = Some(representative);
    }

    // Step 10: Rewrite edge_stash.
    // Remove edges where both endpoints are in the group.
    // Rewrite edges incoming to group members: target.node = representative, target.port = group_input_index.
    // Rewrite edges outgoing from group members: source.node = representative, source.port = group_output_index.
    let mut new_edge_stash: Vec<(Source, Target)> = Vec::new();

    for (source, target) in edge_stash.iter() {
        let src_in = group_set.contains(&source.node);
        let tgt_in = group_set.contains(&target.node);

        if src_in && tgt_in {
            // Internal edge: remove.
            continue;
        } else if !src_in && tgt_in {
            // Incoming edge: rewrite target.
            if let Some(&group_input) = input_port_to_group_input.get(&(target.node, target.port)) {
                new_edge_stash.push((
                    *source,
                    Target::new(representative, group_input),
                ));
            }
        } else if src_in && !tgt_in {
            // Outgoing edge: rewrite source.
            let topo_pos = node_to_topo[&source.node];
            if let Some(group_outs) = output_port_to_group_output.get(&(source.node, source.port)) {
                for &group_out in group_outs {
                    if output_map[group_out] == (topo_pos, source.port) {
                        new_edge_stash.push((
                            Source::new(representative, group_out),
                            *target,
                        ));
                    }
                }
            }
        } else {
            // Neither endpoint in group: keep as-is.
            new_edge_stash.push((*source, *target));
        }
    }

    *edge_stash = new_edge_stash;
}
