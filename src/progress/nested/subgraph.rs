//! A dataflow subgraph
//!
//! Timely dataflow graphs can be nested hierarchically, where some region of
//! graph is grouped, and presents upwards as an operator. This grouping needs
//! some care, to make sure that the presented operator reflects the behavior
//! of the grouped operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use logging::TimelyLogger as Logger;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, Operate, operate::SharedProgress};
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
    TInner: Timestamp,
    TOuter: Timestamp,
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
    TInner: Timestamp+Refines<TOuter>,
    TOuter: Timestamp,
{
    /// Allocates a new input to the subgraph and returns the target to that input in the outer graph.
    pub fn new_input(&mut self, shared_counts: Rc<RefCell<ChangeBatch<TInner>>>) -> Target {
        self.input_messages.push(shared_counts);
        self.children[0].add_output();
        Target { index: self.index, port: self.input_messages.len() - 1 }
    }

    /// Allocates a new output from the subgraph and returns the source of that output in the outer graph.
    pub fn new_output(&mut self) -> Source {
        self.output_capabilities.push(MutableAntichain::new());
        self.children[0].add_input();
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

        let children = vec![PerOperatorState::empty(path.clone(), logging.clone())];

        SubgraphBuilder {
            name:                name.into(),
            path,
            index,

            children,
            child_count:         1,
            edge_stash:          vec![],

            input_messages:      Default::default(),
            output_capabilities: Default::default(),

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

            // pointstamps:               Default::default(),
            // local_pointstamp_messages: ChangeBatch::new(),
            // local_pointstamp_internal: ChangeBatch::new(),
            // final_pointstamp_messages: ChangeBatch::new(),
            // final_pointstamp_internal: ChangeBatch::new(),
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
    TInner: Timestamp+Refines<TOuter>,
    TOuter: Timestamp,
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
    // local_pointstamp_messages: ChangeBatch<(usize, usize, TInner)>,
    // local_pointstamp_internal: ChangeBatch<(usize, usize, TInner)>,
    local_pointstamp: ChangeBatch<(Location, TInner)>,

    // final_pointstamp_messages: ChangeBatch<(usize, usize, TInner)>,
    // final_pointstamp_internal: ChangeBatch<(usize, usize, TInner)>,
    final_pointstamp: ChangeBatch<(Location, TInner)>,

    // Graph structure and pointstamp tracker.
    pointstamp_builder: reachability::Builder<TInner>,
    pointstamp_tracker: reachability::Tracker<TInner>,

    // channel / whatever used to communicate pointstamp updates to peers.
    progcaster: Progcaster<TInner>,

    shared_progress: Rc<RefCell<SharedProgress<TOuter>>>,
}


impl<TOuter, TInner> Operate<TOuter> for Subgraph<TOuter, TInner>
where
    TInner: Timestamp+Refines<TOuter>,
    TOuter: Timestamp,
{

    fn name(&self) -> String { self.name.clone() }
    fn local(&self) -> bool { false }
    fn inputs(&self)  -> usize { self.inputs }
    fn outputs(&self) -> usize { self.outputs }

    // produces connectivity summaries from inputs to outputs, and reports initial internal
    // capabilities on each of the outputs (projecting capabilities from contained scopes).
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Rc<RefCell<SharedProgress<TOuter>>>) {

        // double-check that child 0 (the outside world) is correctly shaped.
        assert_eq!(self.children[0].outputs, self.inputs());
        assert_eq!(self.children[0].inputs, self.outputs());

        // collect the initial capabilities of each child, to determine the initial capabilities
        // of the subgraph.
        for child in self.children.iter_mut() {
            for (output, capability) in child.gis_capabilities.iter_mut().enumerate() {
                for &(ref time, value) in capability.iter() {
                    self.pointstamp_tracker.update_source(Source { index: child.index, port: output }, time.clone(), value);
                }
            }
        }

        // Move pointstamps along paths to arrive at output ports,
        // (the only destination of interest at the moment).
        self.pointstamp_tracker.propagate_all();

        // Capabilities from internal operators have been pushed, and we can now find capabilities
        // for each output port at `self.pointstamps.pushed[0]`. We should promote them to their
        // appropriate buffer (`self.output_capabilities`) and present them as initial capabilities
        // for the subgraph operator (`initial_capabilities`).
        let initial_capabilities = &mut self.shared_progress.borrow_mut().internals[..];
        // let mut initial_capabilities = vec![ChangeBatch::new(); self.outputs()];
        for (o_port, capabilities) in self.pointstamp_tracker.pushed_mut(0).iter_mut().enumerate() {
            let caps1 = capabilities.drain().map(|(time, diff)| (time.to_outer(), diff));
            self.output_capabilities[o_port].update_iter_and(caps1, |t, v| {
                initial_capabilities[o_port].update(t.clone(), v);
            });
        }

        // done with the pointstamps, so we should clean up.
        self.pointstamp_tracker.clear();

        let summary = self.pointstamp_builder.summarize();

        // Summarize the scope internals by looking for source_target_summaries from child 0
        // sources to child 0 targets. These summaries are only in terms of the outer timestamp.
        let mut internal_summary = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];
        for input in 0..self.inputs() {
            for &(target, ref antichain) in &summary.source_target[0][input] {
                if target.index == 0 {
                    for summary in antichain.elements().iter() {
                        internal_summary[input][target.port].insert(TInner::summarize(summary.clone()));
                    };
                }
            }
        }

        (internal_summary, self.shared_progress.clone())
    }

    /// Receive summaries from outputs to inputs, as well as initial external capabilities on inputs.
    ///
    /// This method finalizes the internal reachability of this `Subgraph`, and provides the corresponding
    /// information on to each of its children.
    fn set_external_summary(&mut self) {

        // We must first translate `summaries` to summaries in the subgraph's timestamp type.
        // Each of these summaries correspond to dropping the inner timestamp coordinate and replacing
        // it with the default value, and applying the summary to the outer coordinate.
        // let mut new_summary = vec![vec![Antichain::new(); self.inputs]; self.outputs];
        // for output in 0..self.outputs {
        //     for input in 0..self.inputs {
        //         for summary in summaries[output][input].elements() {
        //             new_summary[output][input].insert(Outer(summary.clone(), Default::default()));
        //         }
        //     }
        // }

        // The element of `frontier` form the initial capabilities of child zero, our proxy for the outside world.
        let mut new_capabilities = vec![ChangeBatch::new(); self.inputs];
        for (index, batch) in self.shared_progress.borrow_mut().frontiers.iter_mut().enumerate() {
            let iterator = batch.drain().map(|(time, value)| (TInner::to_inner(time), value));
            new_capabilities[index].extend(iterator);
        }
        self.children[0].gis_capabilities = new_capabilities;

        // Install the new summary, summarize, and remove "unhelpful" summaries.
        // Specifically, and crucially, we remove summaries from the outputs of child zero to the inputs of child
        // zero. This prevents the subgraph from reporting the external world's capabilities back as capabilities
        // held by the subgraph. We also remove summaries to nodes that do not require progress information.
        // self.pointstamp_builder.add_node(0, self.outputs, self.inputs, new_summary);
        let mut pointstamp_summaries = self.pointstamp_builder.summarize();
        for summaries in pointstamp_summaries.target_target[0].iter_mut() { summaries.retain(|&(t, _)| t.index > 0); }
        for summaries in pointstamp_summaries.source_target[0].iter_mut() { summaries.retain(|&(t, _)| t.index > 0); }
        for child in 0 .. self.children.len() {
            for summaries in pointstamp_summaries.target_target[child].iter_mut() { summaries.retain(|&(t,_)| self.children[t.index].notify); }
            for summaries in pointstamp_summaries.source_target[child].iter_mut() { summaries.retain(|&(t,_)| self.children[t.index].notify); }
        }

        // Allocate the pointstamp tracker using the finalized topology.
        self.pointstamp_tracker = reachability::Tracker::allocate_from(pointstamp_summaries.clone());

        // Initialize all expressed capabilities as pointstamps, for propagation.
        for child in self.children.iter_mut() {
            for output in 0 .. child.outputs {
                let mut child_shared_progress = &mut child.shared_progress.borrow_mut();
                for (time, value) in child_shared_progress.internals[output].drain() {
                    self.pointstamp_tracker.update_source(
                        Source { index: child.index, port: output },
                        time.clone(),
                        value,
                    )
                }
            }
        }

        // Propagate pointstamps using the complete summary to determine initial frontiers for each child.
        self.pointstamp_tracker.propagate_all();

        // We now have enough information to call `set_external_summary` for each child.
        for child in self.children.iter_mut() {

            // // Titrate propagated capability changes through a MutableAntichain, and leave them in
            // // the child's buffer for pending `external` updates to apply in its next `push_external`
            // // call.
            // let pushed_mut = self.pointstamp_tracker.pushed_mut(child.index);
            // for input in 0..child.inputs {
            //     let buffer = &mut child.external_buffer[input];
            //     let iterator2 = pushed_mut[input].drain();
            //     child.external[input].update_iter_and(iterator2, |t, v| { buffer.update(t.clone(), v); });
            // }

            // Summarize the subgraph by the path summaries from the child's output to its inputs.
            let mut summary = vec![vec![Antichain::new(); child.inputs]; child.outputs];
            for output in 0..child.outputs {
                for &(source, ref antichain) in &pointstamp_summaries.source_target[child.index][output] {
                    if source.index == child.index {
                        summary[output][source.port] = (*antichain).clone();
                    }
                }
            }

            let child_index = child.index;
            child.set_external_summary(summary, self.pointstamp_tracker.pushed_mut(child_index));
        }

        // clean up after ourselves.
        assert!(self.pointstamp_tracker.is_empty());
    }

    fn schedule(&mut self) -> bool {

        let shared_progress = &mut *self.shared_progress.borrow_mut();

        for (port, changes) in shared_progress.frontiers.iter_mut().enumerate() {
            for (time, value) in changes.drain() {
                self.pointstamp_tracker.update_source(
                    Source { index: 0, port },
                    TInner::to_inner(time),
                    value
                );
            }
        }

        let consumed = &mut shared_progress.consumeds[..];
        let internal = &mut shared_progress.internals[..];
        let produced = &mut shared_progress.produceds[..];

        // This is a fair hunk of code, which we've broken down into six steps.
        //
        //   Step 1: harvest local data on records entering the scope.
        //   Step 2: exchange progress data with other workers.
        //   Step 3: drain exchanged data into the pointstamp tracker.
        //   Step 4: propagate pointstamp information.
        //   Step 5: inform children of implications, elicit progress info from them.
        //   Step 6: report implications on subgraph outputs as internal capabilities.
        //
        // This operator has a fair bit of control, and should be able to safely perform subsets of this

        // We expect the buffers to populate should be empty.
        // This isn't strictly mandatory, but it would likely be a bug.
        debug_assert!(consumed.iter_mut().all(|x| x.is_empty()));
        debug_assert!(internal.iter_mut().all(|x| x.is_empty()));
        debug_assert!(produced.iter_mut().all(|x| x.is_empty()));

        // Step 1. Harvest data on records entering the scope.
        //
        // These counts are not directly reported upwards as "consumed". Instead, these records
        // are (CHEAT!, CHEAT!) marked as changes in child 0's output capabilities, exchanged
        // with other workers, and extracted at mark [XXX].

        for input in 0 .. self.inputs {
            let mut borrowed = self.input_messages[input].borrow_mut();
            for (time, delta) in borrowed.drain() {
                for target in &self.children[0].edges[input] {
                    self.local_pointstamp.update((Location::from(*target), time.clone()), delta);
                }
                // This is the cheat, which we resolve at mark [XXX] before anyone notices.
                // NB: We negate delta, because these events look like consuming messages,
                //     and are paired with the production of internal messages.
                //     Not *strictly* necessary here, but may be important for sanity when
                //     considering "properties" of batches of updates, or when applying
                //     rules that "allow" the delay of increments v decrements.
                self.local_pointstamp.update((Location::new_source(0, input), time), -delta);
            }
        }

        // Step 2. Exchange local progress information with other workers.
        //
        // We have an opportunity here to *not* exchange local progress information, if we have
        // reasons to believe it is not productive (as it can be expensive). For example, if we
        // know that these changes could not advance the `final_pointstamp` frontier we could
        // safely hold back the changes.
        self.progcaster.send_and_recv(&mut self.local_pointstamp);

        // Step 3. Drain the post-exchange progress information into `self.pointstamp_tracker`.
        //
        // Child-zero statements are extracted and reported either as consumed input records
        // or produced output records. Other statements are introduced to the progress tracker,
        // which will propogate the consequences around the dataflow graph.
        //
        // By reporting consumed input records we *must* apply and report all other progress
        // statements, to make sure that any corresponding internal and produced counts are
        // surfaced at the same time.

        // Drain exchanged pointstamps into "final" pointstamps.
        self.local_pointstamp.drain_into(&mut self.final_pointstamp);

        // Process exchange pointstamps. Handle child 0 statements carefully.
        for ((location, timestamp), delta) in self.final_pointstamp.drain() {

            // Child 0 corresponds to the parent scope and has special handling.
            if location.node == 0 {
                match location.port {
                    Port::Source(scope_input) => {
                        // [XXX] Report child 0's capabilities as consumed messages.
                        //       Note the re-negation of delta, to make counts positive.
                        consumed[scope_input].update(timestamp.to_outer(), -delta);
                    },
                    Port::Target(scope_output) => {
                        // [YYY] Report child 0's messages as produced.
                        //       Do not otherwise record, as we will not see subtractions.
                        produced[scope_output].update(timestamp.to_outer(), delta);
                    }
                }
            }
            else {
                self.pointstamp_tracker.update(location, timestamp, delta);
            }
        }

        // Step 4. Propagate pointstamp updates to inform each source about changes in their frontiers.
        //
        // The most crucial thing that happens here is that the implications for the input frontier of
        // child zero are surfaced, as we will need to report these in `internal`. It is *possible*
        // that we do not have to propagate progress information from all internal operators, but at
        // the moment doing so is substantially safer than trying to be clever.
        self.pointstamp_tracker.propagate_all();

        // Step 5. Provide each child with updated frontier information and an opportunity to execute.
        let mut any_child_active = false;
        for (index, child) in self.children.iter_mut().enumerate().skip(1) {

            // The child should either fill a "yet to be exchanged" buffer of progress updates, or an
            // "already exchanged" buffer, depending on whether it indicates that its results have been
            // exchanged already (through its `local` field).
            let pointstamp_buffer = if child.local {
                &mut self.local_pointstamp
            }
            else {
                &mut self.final_pointstamp
            };

            // Activate the child and harvest its progress updates. Here we pass along a reference
            // to the source in the progress tracker so that the child can determine if it holds
            // any capabilities; if not, it is a candidate for being shut down.

            let (targets, sources, pushed) = self.pointstamp_tracker.node_state(index);

            // This is mis-named, and schedules a child as well as collecting progress data.
            let child_active = child.exchange_progress(
                pushed,
                targets,
                sources,
                pointstamp_buffer,
            );

            any_child_active = any_child_active || child_active;
        }

        // Step 6. Child zero's frontier information are reported as capabilities via `internal`.
        for (output, pointstamps) in self.pointstamp_tracker.pushed_mut(0).iter_mut().enumerate() {
            let iterator = pointstamps.drain().map(|(time, diff)| (time.to_outer(), diff));
            self.output_capabilities[output].update_iter_and(iterator, |t, v| {
                internal[output].update(t.clone(), v);
            });
        }

        // This does not *need* to be true, in that we hope that it is possible to execute correctly
        // even when we leave some pointstamp data behind. In the current implementation, where we
        // propagate all updates and then process each child, all updates should be consumed.
        debug_assert!(self.pointstamp_tracker.is_empty());

        // Report activity if any child does, or our pointstamp tracker is tracking something.
        any_child_active || self.pointstamp_tracker.tracking_anything()
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

    external: Vec<MutableAntichain<T>>, // input capabilities expressed by outer scope

    shared_progress: Rc<RefCell<SharedProgress<T>>>,

    gis_capabilities: Vec<ChangeBatch<T>>,
    gis_summary: Vec<Vec<Antichain<T::Summary>>>,   // cached result from get_internal_summary.

    logging: Option<Logger>,
}

impl<T: Timestamp> PerOperatorState<T> {

    fn add_input(&mut self) {
        self.inputs += 1;
        self.external.push(Default::default());
        self.shared_progress = Rc::new(RefCell::new(SharedProgress::new(self.inputs,self.outputs)));
    }
    fn add_output(&mut self) {
        self.outputs += 1;
        self.edges.push(vec![]);
        self.shared_progress = Rc::new(RefCell::new(SharedProgress::new(self.inputs,self.outputs)));
    }

    fn empty(mut path: Vec<usize>, logging: Option<Logger>) -> PerOperatorState<T> {
        path.push(0);
        PerOperatorState {
            name:       "External".to_owned(),
            // addr:       path,
            operator:      None,
            index:      0,
            id:         usize::max_value(),
            local:      false,
            inputs:     0,
            outputs:    0,

            recently_active: true,
            notify:     true,

            edges: Vec::new(),
            external: Vec::new(),

            logging,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(0,0))),
            gis_capabilities: Vec::new(),
            gis_summary: Vec::new(),
        }
    }

    pub fn new(mut scope: Box<Operate<T>>, index: usize, mut _path: Vec<usize>, identifier: usize, logging: Option<Logger>) -> PerOperatorState<T> {

        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let (gis_summary, shared_progress) = scope.get_internal_summary();
        let gis_capabilities = shared_progress.borrow_mut().internals.clone();

        assert_eq!(gis_summary.len(), inputs);
        assert!(!gis_summary.iter().any(|x| x.len() != outputs));

        PerOperatorState {
            name:               scope.name(),
            operator:           Some(scope),
            index,
            id:                 identifier,
            local,
            inputs,
            outputs,
            edges:              vec![vec![]; outputs],

            recently_active:    true,
            notify,

            external:           vec![Default::default(); inputs],

            logging,

            shared_progress,
            gis_capabilities,
            gis_summary,
        }
    }

    pub fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<T::Summary>>>, capabilities: &mut [ChangeBatch<T>]) {

        debug_assert_eq!(self.shared_progress.borrow_mut().frontiers.len(), capabilities.len());

        // Filter initial capabilities through `MutableAntichain`, producing only the discrete
        // changes in `self.external_buffer`
        for input in 0 .. self.inputs {
            let buffer = &mut self.shared_progress.borrow_mut().frontiers[input];
            self.external[input].update_iter_and(capabilities[input].drain(), |t, v| {
                buffer.update(t.clone(), v);
            });
        }

        // If we initialize an operator with inputs that cannot receive data, that could very likely be a bug.
        //
        // NOTE: This may not be a bug should we move to messages with capability sets, where one option is an
        //       empty set. This could mean a channel that transmits data but not capabilities, would would
        //       here appear as an input with no initial capabilities.
        if self.index > 0 && self.notify && !self.shared_progress.borrow_mut().frontiers.is_empty() && !self.shared_progress.borrow_mut().frontiers.iter_mut().any(|x| !x.is_empty()) {
            println!("initializing notifiable operator {}[{}] with inputs but no external capabilities", self.name, self.index);
        }

        // Pass the summary and filtered capabilities to the operator.
        if let Some(ref mut scope) = self.operator {
            scope.set_external_summary();//summaries, &mut self.external_buffer);
        }
    }

    pub fn exchange_progress(
        &mut self,
        external_progress: &mut [ChangeBatch<T>],       // changes to external capabilities on operator inputs.
        _outstanding_messages: &[MutableAntichain<T>],   // the reported outstanding messages to the operator.
        internal_capabilities: &[MutableAntichain<T>],  // the reported internal capabilities of the operator.
        // pointstamp_messages: &mut ChangeBatch<(usize, usize, T)>,
        // pointstamp_internal: &mut ChangeBatch<(usize, usize, T)>,
        pointstamps: &mut ChangeBatch<(Location, T)>,
    ) -> bool {

        let active = if let Some(ref mut operator) = self.operator {

            // We must filter the changes through a `MutableAntichain` to determine discrete changes in the
            // input capabilities, given all pre-existing updates accepted and communicated.
            for (input, updates) in external_progress.iter_mut().enumerate() {
                let buffer = &mut self.shared_progress.borrow_mut().frontiers[input];
                self.external[input].update_iter_and(updates.drain(), |time, val| {
                    buffer.update(time.clone(), val);
                });
            }

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

                let self_id = self.id;  // avoid capturing `self` in logging closures.

                if any_progress_updates {
                    self.logging.as_mut().map(|l| {
                        l.log(::logging::PushProgressEvent {
                            op_id: self_id,
                        });
                    });
                }

                self.logging.as_mut().map(|l| l.log(::logging::ScheduleEvent {
                    id: self_id, start_stop: ::logging::StartStop::Start
                }));

                debug_assert!(self.shared_progress.borrow_mut().consumeds.iter_mut().all(|cm| cm.is_empty()));
                debug_assert!(self.shared_progress.borrow_mut().internals.iter_mut().all(|cm| cm.is_empty()));
                debug_assert!(self.shared_progress.borrow_mut().produceds.iter_mut().all(|cm| cm.is_empty()));

                let internal_activity = operator.schedule();

                let shared_progress = &mut *self.shared_progress.borrow_mut();

                let did_work =
                    shared_progress.consumeds.iter_mut().any(|x| !x.is_empty()) ||
                    shared_progress.internals.iter_mut().any(|x| !x.is_empty()) ||
                    shared_progress.produceds.iter_mut().any(|x| !x.is_empty());

                for (input, consumed) in shared_progress.consumeds.iter_mut().enumerate() {
                    for (time, delta) in consumed.drain() {
                        pointstamps.update((Location::new_target(self.index, input), time), -delta);
                    }
                }
                for (output, internal) in shared_progress.internals.iter_mut().enumerate() {
                    for (time, delta) in internal.drain() {
                        pointstamps.update((Location::new_source(self.index, output), time.clone()), delta);
                    }
                }
                // Scan reported changes, propagate as appropriate.
                for (output, produced) in shared_progress.produceds.iter_mut().enumerate() {
                    for (time, delta) in produced.drain() {
                        for target in &self.edges[output] {
                            pointstamps.update((Location::from(*target), time.clone()), delta);
                        }
                    }
                }

                // The operator was recently active if it did anything, or reports activity.
                self.recently_active = did_work || internal_activity;

                self.logging.as_mut().map(|l|
                    l.log(::logging::ScheduleEvent {
                        id: self_id,
                        start_stop: ::logging::StartStop::Stop { activity: did_work }
                    }));

                internal_activity
            }
            else {
                // Active operators should always be scheduled, and should re-assert their activity if
                // they want to be scheduled again. If we are here, it is because the operator declined
                // to express activity explicitly.
                false
            }
        }
        else {

            // If the operator is closed and we are reporting progress at it, something has surely gone wrong.
            if !external_progress.iter_mut().all(|x| x.is_empty()) {
                println!("Operator prematurely shut down: {}", self.name);
                println!("  {:?}", self.notify);
                println!("  {:?}", external_progress);
            }
            assert!(external_progress.iter_mut().all(|x| x.is_empty()));

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
           self.notify && self.external.iter().all(|x| x.is_empty()) &&
           internal_capabilities.iter().all(|x| x.is_empty()) {
               self.operator = None;
               self.name = format!("{}(tombstone)", self.name);
           }

        active
    }
}
