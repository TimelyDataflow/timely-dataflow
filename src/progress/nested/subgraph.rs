//! Implements `Operate` for a scoped collection of child operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use timely_communication::Allocate;

use logging::Logger;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, Operate};

use progress::ChangeBatch;
use progress::broadcast::Progcaster;
use progress::nested::summary::Summary::{Local, Outer};
use progress::nested::product::Product;
use progress::nested::reachability;

// IMPORTANT : by convention, a child identifier of zero is used to indicate inputs and outputs of
// the Subgraph itself. An identifier greater than zero corresponds to an actual child, which can
// be found at position (id - 1) in the `children` field of the Subgraph.

/// Names a source of a data stream.
///
/// A source of data is either a child output, or an input from a parent.
/// Conventionally, `index` zero is used for parent input.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Source { 
    /// Index of the source operator.
    pub index: usize, 
    /// Number of the output port from the operator.
    pub port: usize, 
}

/// Names a target of a data stream.
///
/// A target of data is either a child input, or an output to a parent.
/// Conventionally, `index` zero is used for parent output.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Target { 
    /// Index of the target operator.
    pub index: usize, 
    /// Nmuber of the input port to the operator.
    pub port: usize, 
}

/// A builder structure for initializing `Subgraph`s.
///
/// This collects all the information necessary to get a `Subgraph` up and
/// running, and is important largely through its `build` method which
/// actually creates a `Subgraph`.
pub struct SubgraphBuilder<TOuter: Timestamp, TInner: Timestamp> {
    /// The name of this subgraph.
    pub name: String,

    /// A sequence of integers uniquely identifying the subgraph.
    pub path: Vec<usize>,

    /// The index assigned to the subgraph by its parent.
    index: usize,

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<Product<TOuter, TInner>>>,
    child_count: usize,

    edge_stash: Vec<(Source, Target)>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<ChangeBatch<Product<TOuter, TInner>>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    /// Logging handle
    logging: Logger,
}

impl<TOuter: Timestamp, TInner: Timestamp> SubgraphBuilder<TOuter, TInner> {
    /// Allocates a new input to the subgraph and returns the target to that input in the outer graph.
    pub fn new_input(&mut self, shared_counts: Rc<RefCell<ChangeBatch<Product<TOuter, TInner>>>>) -> Target {
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
    pub fn new_from(index: usize, mut path: Vec<usize>, logging: Logger) -> SubgraphBuilder<TOuter, TInner> {
        path.push(index);

        let children = vec![PerOperatorState::empty(path.clone(), logging.clone())];

        SubgraphBuilder {
            name:                   "Subgraph".into(),
            path:                   path,
            index:                  index,

            children:               children,
            child_count:            1,
            edge_stash: vec![],

            input_messages:         Default::default(),
            output_capabilities:    Default::default(),

            logging:                logging,
        }
    }

    /// Allocates a new child identifier, for later use.
    pub fn allocate_child_id(&mut self) -> usize {
        self.child_count += 1;
        self.child_count - 1
    }

    /// Adds a new child to the subgraph.
    pub fn add_child(&mut self, child: Box<Operate<Product<TOuter, TInner>>>, index: usize, identifier: usize) {
        {
            let mut child_path = self.path.clone();
            child_path.push(index);
            self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::Operates(::logging::OperatesEvent {
                id: identifier,
                addr: child_path,
                name: child.name().to_owned(),
            })));
        }
        self.children.push(PerOperatorState::new(child, index, self.path.clone(), identifier, self.logging.clone()))
    }

    /// Now that initialization is complete, actually build a subgraph.
    pub fn build<A: Allocate>(mut self, allocator: &mut A) -> Subgraph<TOuter, TInner> {
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

        let progcaster = Progcaster::new(allocator, &self.path, self.logging.clone());

        Subgraph {
            name: self.name,
            path: self.path,
            inputs: self.input_messages.len(),
            outputs: self.output_capabilities.len(),
            children: self.children,
            input_messages: self.input_messages,
            output_capabilities: self.output_capabilities,

            // pointstamps:               Default::default(),
            local_pointstamp_messages: ChangeBatch::new(),
            local_pointstamp_internal: ChangeBatch::new(),
            final_pointstamp_messages: ChangeBatch::new(),
            final_pointstamp_internal: ChangeBatch::new(),
            progcaster:                progcaster,

            pointstamp_builder: builder,
            pointstamp_tracker: tracker,
        }
    }
}


/// A dataflow subgraph.
///
/// The subgraph type contains the infrastructure required to describe the topology of and track
/// progress within a dataflow subgraph. 
pub struct Subgraph<TOuter:Timestamp, TInner:Timestamp> {

    name: String,           // a helpful name (often "Subgraph").
    /// Path of identifiers from the root.
    pub path: Vec<usize>,   
    inputs: usize,          // number of inputs.
    outputs: usize,         // number of outputs.

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<Product<TOuter, TInner>>>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<ChangeBatch<Product<TOuter, TInner>>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    // pointstamp messages to exchange. ultimately destined for `messages` or `internal`.
    local_pointstamp_messages: ChangeBatch<(usize, usize, Product<TOuter, TInner>)>,
    local_pointstamp_internal: ChangeBatch<(usize, usize, Product<TOuter, TInner>)>,
    final_pointstamp_messages: ChangeBatch<(usize, usize, Product<TOuter, TInner>)>,
    final_pointstamp_internal: ChangeBatch<(usize, usize, Product<TOuter, TInner>)>,

    // Graph structure and pointstamp tracker.
    pointstamp_builder: reachability::Builder<Product<TOuter, TInner>>,
    pointstamp_tracker: reachability::Tracker<Product<TOuter, TInner>>,

    // channel / whatever used to communicate pointstamp updates to peers.
    progcaster: Progcaster<Product<TOuter, TInner>>,
}


impl<TOuter: Timestamp, TInner: Timestamp> Operate<TOuter> for Subgraph<TOuter, TInner> {

    fn name(&self) -> String { self.name.clone() }
    fn local(&self) -> bool { false }
    fn inputs(&self)  -> usize { self.inputs }
    fn outputs(&self) -> usize { self.outputs }

    // produces connectivity summaries from inputs to outputs, and reports initial internal
    // capabilities on each of the outputs (projecting capabilities from contained scopes).
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Vec<ChangeBatch<TOuter>>) {

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
        let mut initial_capabilities = vec![ChangeBatch::new(); self.outputs()];
        for (o_port, capabilities) in self.pointstamp_tracker.pushed_mut(0).iter_mut().enumerate() {
            let caps1 = capabilities.drain().map(|(time, diff)| (time.outer, diff));
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
                        internal_summary[input][target.port].insert(match summary {
                            &Local(_)    => Default::default(),
                            &Outer(ref y, _) => y.clone(),
                        });
                    };
                }
            }
        }

        (internal_summary, initial_capabilities)
    }

    // receives connectivity summaries from outputs to inputs, as well as initial external
    // capabilities on inputs. prepares internal summaries, and recursively calls the method on
    // contained scopes.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<TOuter::Summary>>>, frontier: &mut [ChangeBatch<TOuter>]) {

        // Translate `summaries` to summaries of the subgraph's timestamp type.
        let mut new_summary = vec![vec![Antichain::new(); self.inputs]; self.outputs];
        for output in 0..self.outputs {
            for input in 0..self.inputs {
                for summary in summaries[output][input].elements() {
                    new_summary[output][input].insert(Outer(summary.clone(), Default::default()));
                }
            }
        }

        let mut new_capabilities = vec![ChangeBatch::new(); self.inputs];
        for (index, batch) in frontier.iter_mut().enumerate() {
            let iterator = batch.drain().map(|(time, value)| (Product::new(time, Default::default()), value));
            new_capabilities[index].extend(iterator);
        }
        self.children[0].gis_capabilities = new_capabilities;

        // Install the new summary, summarize, and remove "unhelpful" summaries.
        self.pointstamp_builder.add_node(0, self.outputs, self.inputs, new_summary);
        let mut pointstamp_summaries = self.pointstamp_builder.summarize();
        for summaries in pointstamp_summaries.target_target[0].iter_mut() { summaries.retain(|&(t, _)| t.index > 0); }
        for summaries in pointstamp_summaries.source_target[0].iter_mut() { summaries.retain(|&(t, _)| t.index > 0); }
        for child in 0 .. self.children.len() {
            for summaries in pointstamp_summaries.target_target[child].iter_mut() { summaries.retain(|&(t,_)| self.children[t.index].notify); }
            for summaries in pointstamp_summaries.source_target[child].iter_mut() { summaries.retain(|&(t,_)| self.children[t.index].notify); }
        }

        self.pointstamp_tracker = reachability::Tracker::allocate_from(pointstamp_summaries.clone());

        // Initialize all expressed capablities as pointstamps, for propagation.
        for child in self.children.iter_mut() {
            for output in 0 .. child.outputs {
                for &(ref time, value) in child.gis_capabilities[output].iter() {
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
        for child in &mut self.children {

            // Initial capabilities from self.pointstamps.target_pushed[child.index]
            // TODO : Should we push anything at child 0? kind of a (small) waste...
            let pushed_mut = self.pointstamp_tracker.pushed_mut(child.index);
            for input in 0..child.inputs {
                let buffer = &mut child.external_buffer[input];
                let iterator2 = pushed_mut[input].drain();
                child.external[input].update_iter_and(iterator2, |t, v| { buffer.update(t.clone(), v); });
            }

            // Summarize the subgraph by the path summaries from the child's output to its inputs.
            let mut summary = vec![vec![Antichain::new(); child.inputs]; child.outputs];
            for output in 0..child.outputs {
                for &(source, ref antichain) in &pointstamp_summaries.source_target[child.index][output] {
                    if source.index == child.index {
                        summary[output][source.port] = (*antichain).clone();
                    }
                }
            }

            child.set_external_summary(summary);
        }

        // clean up after ourselves.
        assert!(self.pointstamp_tracker.is_empty());
    }

    // changes in the message possibilities for each of the subgraph's inputs.
    fn push_external_progress(&mut self, external: &mut [ChangeBatch<TOuter>]) {

        // I believe we can simply move these into our pointstamp staging area.
        // Nothing will happen until we call `step`, but that is to be expected.
        for (port, changes) in external.iter_mut().enumerate() {
            for (time, value) in changes.drain() {
                self.pointstamp_tracker.update_source(
                    Source { index: 0, port: port }, 
                    Product::new(time, Default::default()), 
                    value
                );
            }
        }

        // This update should reflect messages reported in `produced`, so we remove them now.
        // It would be better if the parent explicitly reported how many were consumed, much
        // like how we report to the parent how many input records were consumed.
        self.pointstamp_tracker.target(0).iter_mut().for_each(|x| x.clear());
    }

    /// Report changes in messages and capabilities for the subgraph and its peers.
    ///
    /// This method populates its arguments with accumulated changes across all of its peers, indicating
    /// input messages consumed, internal capabilities held or dropped, and output messages produced.
    ///
    /// Importantly, these changes are aggregated across all peers, reflecting the current information
    /// received by the operator from its own internal progress tracking. This has the potential to lead
    /// to confusing conclusions (from experience), and should be treated carefully and with as much rigor
    /// as possible. This behavior is indicated to others by the `self.local()` method returning `false`.
    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<TOuter>],
                                         internal: &mut [ChangeBatch<TOuter>],
                                         produced: &mut [ChangeBatch<TOuter>]) -> bool
    {
        // This is a fair hunk of code, which we've broken down into six steps.
        //
        //   Step 1: harvest data on records entering the scope.
        //   Step 2: exchange progress data with other workers.
        //   Step 3: drain exchanged data into the pointstamp tracker.
        //   Step 4: propagate pointstamp information.
        //   Step 5: inform children of implications, elicit progress info from them.
        //   Step 6: report implications on subgraph outputs as internal capabilities.
        //
        // This operator has a fair bit of control, and should be able to safely perform subsets of this
        // The main non-obvious correctness requirement is that once we extract progress information from
        // a child we must reflect that information in the next call to the child. Most commonly, this is
        // an issue with a child's `produced`, as once we extract that information the child has no idea
        // whether the associated records still exist in the system, and relies on us to transactionally
        // correct its understanding.

        // We expect the buffers to populate should be empty.
        debug_assert!(consumed.iter_mut().all(|x| x.is_empty()));
        debug_assert!(internal.iter_mut().all(|x| x.is_empty()));
        debug_assert!(produced.iter_mut().all(|x| x.is_empty()));

        // Step 1. We first harvest data on records entering the scope, which will be reported upwards as 
        //         "consumed". These records are also marked as changes in child 0's output capabilities,
        //         which is a bit of a cheat; we use this empty slot to exchange this information with the
        //         other workers, so that each can report the aggregate messages consumed upwards.

        for input in 0..self.inputs {
            let mut borrowed = self.input_messages[input].borrow_mut();
            for (time, delta) in borrowed.drain() {
                for target in &self.children[0].edges[input] {
                    self.local_pointstamp_messages.update((target.index, target.port, time.clone()), delta);
                }
                // This is the cheat, which we resolve at mark [XXX] before anyone notices.
                self.local_pointstamp_internal.update((0, input, time), delta);
            }
        }

        // Step 2. We harvest progress information from other workers. This also currently involves sending
        //         information to other workers, which we may want to delay until we have harvested progress
        //         information from children.
        //
        //         NB: we only exchange `self.local_` updates, as these are the "pre-exchange" updates that
        //         have not already been shuffled. Some updates, from children which do not set `self.local`,
        //         have already been exchange among workers and should not be reshuffled.

        self.progcaster.send_and_recv(
            &mut self.local_pointstamp_messages,
            &mut self.local_pointstamp_internal,
        );

        // Step 3. We drain the post-exchange progress information into `self.pointstamp_tracker`. Along the
        //         way we extract the cheating child zero capabilities, and report aggregate consumed input 
        //         records and produced output records upwards via `consumed` and `produced`, respectively.

        self.local_pointstamp_messages.drain_into(&mut self.final_pointstamp_messages);
        for ((index, port, timestamp), delta) in self.local_pointstamp_internal.drain() {
            if index == 0 {     // [XXX] Report child 0's capabilities as consumed messages.
                consumed[port].update(timestamp.outer, delta);
            }
            else {              // Fold non-cheating capability changes into the final updates.
                self.final_pointstamp_internal.update((index, port, timestamp), delta);
            }
        }

        // Demultiplex `self.final_` into `self.pointstamp_tracker`. Updates to message counts for
        // inputs to child zero are also deposited in `produced`.
        for ((index, input, time), delta) in self.final_pointstamp_messages.drain() {
            if index == 0 { produced[input].update(time.outer.clone(), delta); }
            self.pointstamp_tracker.update_target(Target { index: index, port: input }, time, delta);
        }
        for ((index, output, time), delta) in self.final_pointstamp_internal.drain() {
            self.pointstamp_tracker.update_source(Source { index: index, port: output }, time, delta);
        }

        // Step 4. Propagate pointstamp updates to inform each source about changes in their frontiers.
        self.pointstamp_tracker.propagate_all();

        // Step 5. Provide each child with updated frontier information and an opportunity to execute.
        let mut any_child_active = false;
        for (index, child) in self.children.iter_mut().enumerate().skip(1) {

            child.push_pointstamps(self.pointstamp_tracker.pushed_mut(index));

            // Local children must have results exchanged, and so fill a different buffer.
            let (message_buffer, internal_buffer) = if child.local {
                (&mut self.local_pointstamp_messages, &mut self.local_pointstamp_internal)
            }
            else {
                (&mut self.final_pointstamp_messages, &mut self.final_pointstamp_internal)
            };

            let child_active = child.pull_pointstamps(
                &self.pointstamp_tracker.source(index), 
                message_buffer, 
                internal_buffer);

            any_child_active = any_child_active || child_active;
        }

        // Step 6. Child zero's frontier information are reported as capabilities via `internal`.
        for (output, pointstamps) in self.pointstamp_tracker.pushed_mut(0).iter_mut().enumerate() {
            let iterator = pointstamps.drain().map(|(time, diff)| (time.outer, diff));
            self.output_capabilities[output].update_iter_and(iterator, |t, v| {
                internal[output].update(t.clone(), v);
            });
        }

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

    operator: Option<Box<Operate<T>>>,

    edges: Vec<Vec<Target>>,    // edges from the outputs of the operator

    external: Vec<MutableAntichain<T>>, // input capabilities expressed by outer scope

    consumed_buffer: Vec<ChangeBatch<T>>, // per-input: temp buffer used for pull_internal_progress.
    internal_buffer: Vec<ChangeBatch<T>>, // per-output: temp buffer used for pull_internal_progress.
    produced_buffer: Vec<ChangeBatch<T>>, // per-output: temp buffer used for pull_internal_progress.

    external_buffer: Vec<ChangeBatch<T>>, // per-input: temp buffer used for push_external_progress.

    gis_capabilities: Vec<ChangeBatch<T>>,
    gis_summary: Vec<Vec<Antichain<T::Summary>>>,   // cached result from get_internal_summary.

    logging: Logger,
}

impl<T: Timestamp> PerOperatorState<T> {

    fn add_input(&mut self) {
        self.inputs += 1;
        self.external.push(Default::default());
        self.external_buffer.push(ChangeBatch::new());
        self.consumed_buffer.push(ChangeBatch::new());
    }
    fn add_output(&mut self) {
        self.outputs += 1;
        self.edges.push(vec![]);
        self.internal_buffer.push(ChangeBatch::new());
        self.produced_buffer.push(ChangeBatch::new());
    }

    fn empty(mut path: Vec<usize>, logging: Logger) -> PerOperatorState<T> {
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

            notify:     true,

            edges: Vec::new(),
            external: Vec::new(),
            external_buffer: Vec::new(),
            consumed_buffer: Vec::new(),
            internal_buffer: Vec::new(),
            produced_buffer: Vec::new(),

            logging: logging,

            gis_capabilities: Vec::new(),
            gis_summary: Vec::new(),
        }
    }

    pub fn new(mut scope: Box<Operate<T>>, index: usize, mut _path: Vec<usize>, identifier: usize, logging: Logger) -> PerOperatorState<T> {

        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let (gis_summary, gis_capabilities) = scope.get_internal_summary();

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

            notify,

            external:           vec![Default::default(); inputs],

            external_buffer:    vec![ChangeBatch::new(); inputs],

            consumed_buffer:    vec![ChangeBatch::new(); inputs],
            internal_buffer:    vec![ChangeBatch::new(); outputs],
            produced_buffer:    vec![ChangeBatch::new(); outputs],

            logging,

            gis_capabilities,
            gis_summary,
        }
    }

    pub fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<T::Summary>>>) {
        let frontier = &mut self.external_buffer;
        if self.index > 0 && self.notify && !frontier.is_empty() && !frontier.iter_mut().any(|x| !x.is_empty()) {
            println!("initializing notifiable operator {}[{}] with inputs but no external capabilities", self.name, self.index);
        }
        if let Some(ref mut scope) = self.operator {
            scope.set_external_summary(summaries, frontier);   
        }
    }

    pub fn push_pointstamps(&mut self, external_progress: &mut [ChangeBatch<T>]) {

        // we shouldn't be pushing progress updates at finished operators. would be a bug!
        if !self.operator.is_some() && !external_progress.iter_mut().all(|x| x.is_empty()) {
            println!("Operator prematurely shut down: {}", self.name);
            println!("  {:?}", self.notify);
            println!("  {:?}", external_progress);
        }
        assert!(self.operator.is_some() || external_progress.iter_mut().all(|x| x.is_empty()));

        // TODO : re-introduce use of self.notify
        assert_eq!(external_progress.len(), self.external.len());
        assert_eq!(external_progress.len(), self.external_buffer.len());

        let mut any_changes = false;

        for (input, updates) in external_progress.iter_mut().enumerate() {
            let buffer = &mut self.external_buffer[input];
            self.external[input].update_iter_and(updates.drain(), |time, val| { 
                any_changes = true;
                buffer.update(time.clone(), val); 
            });
        }

        if any_changes {
            let id = self.id;
            self.logging.when_enabled(|l| {
                l.log(::logging::TimelyEvent::PushProgress(::logging::PushProgressEvent {
                    op_id: id,
                }));
            });
        }

        let changes = &mut self.external_buffer;
        self.operator.as_mut().map(|x| x.push_external_progress(changes));

        // Possibly logic error if operator does not read its changes.
        if changes.iter_mut().any(|x| !x.is_empty()) {
            println!("changes not consumed by {:?}", self.name);
        }
        debug_assert!(!changes.iter_mut().any(|x| !x.is_empty()));
        debug_assert!(external_progress.iter_mut().all(|x| x.is_empty()));
    }

    pub fn pull_pointstamps(
        &mut self,
        internal_capabilities: &[MutableAntichain<T>],
        pointstamp_messages: &mut ChangeBatch<(usize, usize, T)>,
        pointstamp_internal: &mut ChangeBatch<(usize, usize, T)>) -> bool {

        let active = {

            self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::Schedule(::logging::ScheduleEvent {
                id: self.id, start_stop: ::logging::StartStop::Start
            })));

            debug_assert!(self.consumed_buffer.iter_mut().all(|cm| cm.is_empty()));
            debug_assert!(self.internal_buffer.iter_mut().all(|cm| cm.is_empty()));
            debug_assert!(self.produced_buffer.iter_mut().all(|cm| cm.is_empty()));

            let result = if let Some(ref mut operator) = self.operator {
                operator.pull_internal_progress(
                    &mut self.consumed_buffer[..],
                    &mut self.internal_buffer[..],
                    &mut self.produced_buffer[..],
                )
            }
            else { false };

            let consumed_buffer = &mut self.consumed_buffer;
            let internal_buffer = &mut self.internal_buffer;
            let produced_buffer = &mut self.produced_buffer;
            let id = self.id;
            self.logging.when_enabled(|l| {
                let did_work =
                    consumed_buffer.iter_mut().any(|cm| !cm.is_empty()) ||
                        internal_buffer.iter_mut().any(|cm| !cm.is_empty()) ||
                        produced_buffer.iter_mut().any(|cm| !cm.is_empty());
                l.log(::logging::TimelyEvent::Schedule(::logging::ScheduleEvent {
                    id,
                    start_stop: ::logging::StartStop::Stop { activity: did_work }
                }));
            });

            result
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

        // for each output: produced messages and internal progress
        for output in 0..self.outputs {
            for (time, delta) in self.produced_buffer[output].drain() {
                for target in &self.edges[output] {
                    pointstamp_messages.update((target.index, target.port, time.clone()), delta);
                }
            }

            for (time, delta) in self.internal_buffer[output].drain() {
                let index = self.index;
                pointstamp_internal.update((index, output, time.clone()), delta);
            }
        }

        // for each input: consumed messages
        for input in 0..self.inputs {
            for (time, delta) in self.consumed_buffer[input].drain() {
                pointstamp_messages.update((self.index, input, time), -delta);
            }
        }

        active
    }
}
