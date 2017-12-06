//! Implements `Operate` for a scoped collection of child operators.

use std::default::Default;
use std::fmt::Debug;

use std::rc::Rc;
use std::cell::RefCell;
use timely_communication::Allocate;

use order::PartialOrder;

use logging::Logger;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, PathSummary, Operate};

use progress::ChangeBatch;

use progress::broadcast::Progcaster;
use progress::nested::summary::Summary;
use progress::nested::summary::Summary::{Local, Outer};
// use progress::nested::scope_wrapper::ChildWrapper;
use progress::nested::pointstamp_counter::PointstampCounter;
use progress::nested::product::Product;

// IMPORTANT : by convention, a child identifier of zero is used to indicate inputs and outputs of
// the Subgraph itself. An identifier greater than zero corresponds to an actual child, which can
// be found at position (id - 1) in the `children` field of the Subgraph.

/// Names a source of a data stream.
///
/// A source of data is either a child output, or an input from a parent.
/// Conventionally, `index` zero is used for parent input.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
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
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
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

/// A dataflow subgraph.
///
/// The subgraph type contains the infrastructure required to describe the topology of and track
/// progress within a dataflow subgraph. 
pub struct Subgraph<TOuter:Timestamp, TInner:Timestamp> {

    name: String,

    /// A sequence of integers uniquely identifying the subgraph.
    pub path: Vec<usize>,

    inputs: usize,
    outputs: usize,

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

    // something about staging to make pointstamp propagation easier. I'll probably remember as I
    // walk through all this again. ><
    pointstamps: PointstampCounter<Product<TOuter, TInner>>,

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
        for child in &self.children {
            self.pointstamps.allocate_for_operator(child.inputs, child.outputs);
            for (output, capability) in child.internal.iter().enumerate() {
                for time in capability.frontier() {
                    self.pointstamps.source[child.index][output].update(time.clone(), 1);
                }
            }
        }

        // having loaded initial capabilities at each child output, we next determine the summaries
        // between all locations and all targets, and then propagate the initial capabilities to the
        // subgraph outputs (child 0's targets).

        self.compute_summaries();   // determine summaries in the absence of external connectivity.
        self.push_pointstamps();    // push capabilities forward to determine output capabilities.

        // Capabilities from internal operators have been pushed, and we can now find capabilities
        // for each output port at `self.pointstamps.pushed[0]`. We should promote them to their 
        // appropriate buffer (`self.output_capabilities`) and present them as initial capabilities
        // for the subgraph operator (`initial_capabilities`).
        let mut initial_capabilities = vec![ChangeBatch::new(); self.outputs()];
        for (o_port, capabilities) in self.pointstamps.pushed[0].iter_mut().enumerate() {

            // each element of capabilities will eventually be subtracted, so we must use them
            // all for inititialization.
            let iterator = capabilities.drain().map(|(time, diff)| (time.outer, diff) );
            self.output_capabilities[o_port].update_iter(iterator);

            // the frontier of output_capabilities should be expressed as initial capabilities.
            for time in self.output_capabilities[o_port].frontier() {
                initial_capabilities[o_port].update(time.clone(), 1);
            }
        }
        // done with the pointstamps, so we should clean up.
        self.pointstamps.clear();

        // Summarize the scope internals by looking for source_target_summaries from child 0
        // sources to child 0 targets. These summaries are only in terms of the outer timestamp.
        let mut internal_summary = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];
        for input in 0..self.inputs() {
            for &(target, ref antichain) in &self.children[0].source_target_summaries[input] {
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

        // we must now finish the work of setting up the subgraph, using the external summary and
        // external capabilities on the subgraph's inputs.

        // First, we install the external summary as if it were child 0's internal summary.
        for output in 0..self.outputs {
            for input in 0..self.inputs {
                for summary in summaries[output][input].elements() {
                    try_to_add_summary(
                        &mut self.children[0].target_source_summaries[output],
                        Source { index: 0, port: input },
                        Outer(summary.clone(), Default::default())
                    );
                }
            }
        }

        // Now re-compute all summaries with child 0's internal summary set.
        self.compute_summaries();

        // filter out the external -> external links, as we only want to summarize contained scopes
        for summaries in &mut self.children[0].target_target_summaries {
            summaries.retain(|&(t, _)| t.index > 0);
        }
        for summaries in &mut self.children[0].source_target_summaries {
            summaries.retain(|&(t, _)| t.index > 0);
        }

        // Initialize pointstamps for capabilities, from `frontier` and from child capabitilies.

        // Install the supplied external capabilities as child 0's internal capabilities.
        for input in 0..self.inputs {
            let iterator = frontier[input].drain().map(|(time, diff)| (Product::new(time, Default::default()), diff));
            self.children[0].internal[input].update_iter(iterator);
        }

        // Initialize all expressed capablities as pointstamps, for propagation.
        for child in &self.children {
            for output in 0..child.outputs {
                for time in child.internal[output].frontier().iter() {
                    self.pointstamps.update_source(
                        Source { index: child.index, port: output },
                        time.clone(),
                        1
                    );
                }
            }
        }

        // Propagate pointstamps using the complete summary to determine initial frontiers for each child.
        self.push_pointstamps();

        // We now have enough information to call `set_external_summary` for each child.
        for child in &mut self.children {

            // Initial capabilities from self.pointstamps.target_pushed[child.index]
            // TODO : Should we push anything at child 0? kind of a (small) waste...
            for input in 0..child.inputs {
                let buffer = &mut child.external_buffer[input];
                let iterator = self.pointstamps.pushed[child.index][input].drain();
                child.external[input].update_iter_and(iterator, |t, v| { buffer.update(t.clone(), v); });
            }

            // Summarize the subgraph by the path summaries from the child's output to its inputs.
            let mut summary = vec![vec![Antichain::new(); child.inputs]; child.outputs];
            for output in 0..child.outputs {
                for &(source, ref antichain) in &child.source_target_summaries[output] {
                    if source.index == child.index {
                        summary[output][source.port] = (*antichain).clone();
                    }
                }
            }

            child.set_external_summary(summary);
        }

        // clean up after ourselves.
        self.pointstamps.clear();
    }

    // changes in the message possibilities for each of the subgraph's inputs.
    fn push_external_progress(&mut self, external: &mut [ChangeBatch<TOuter>]) {

        // I believe we can simply move these into our pointstamp staging area.
        // Nothing will happen until we call `step`, but that is to be expected.
        for (port, changes) in external.iter_mut().enumerate() {

            let pointstamps = &mut self.pointstamps;
            let iterator = changes.drain().map(|(time, diff)| (Product::new(time, Default::default()), diff));
            self.children[0].internal[port].update_iter_and(iterator, |t, v| {
                pointstamps.update_source(Source { index: 0, port: port }, t.clone(), v);
            });
        }

        // we should also take this opportunity to clean out any messages in self.children[0].messages.
        // it isn't exactly correct that we are sure that they have been acknowledged, but ... we don't
        // have enough information in the api at the moment to be certain. we could add it, but for now
        // we assume that a call to `push_external_progress` reflects all updates previously produced by
        // `pull_internal_progress`.
        for (port, messages) in self.children[0].messages.iter_mut().enumerate() {
            for time in messages.frontier() {
                self.pointstamps.update_target(Target { index: 0, port: port }, time.clone(), -1)
            }
            messages.clear();
        }
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
        // We expect the buffers to populate should be empty.
        debug_assert!(consumed.iter_mut().all(|x| x.is_empty()));
        debug_assert!(internal.iter_mut().all(|x| x.is_empty()));
        debug_assert!(produced.iter_mut().all(|x| x.is_empty()));

        // We track whether there is any non-progress reason to keep the subgraph active.
        let mut active = false;

        // Step 1: Observe the number of records this instances has accepted as input.
        //
        // These timestamp counts correspond to records this subgraph instance has "consumed". As such,
        // we must both (i) accept these counts as new records on the internal edges leading from the
        // corresponding input, and also (ii) we must exchange this information with peer instances so
        // that they can also report the same changes to timestamp counts upwards.
        //
        // The second step will be somewhat non-standard, in that we will use the "input capabilities"
        // as a proxy for records consumed (in fact, we are simply told what these changes are in the
        // `push_external_progress` method, and have no reason to exchange this information, leaving a
        // slot in which we can put our data).
        for input in 0..self.inputs {
            let mut borrowed = self.input_messages[input].borrow_mut();
            for (time, delta) in borrowed.drain() {
                // The internal count for { child: 0, port: input } corresponds to the consumed records
                // from `input`. We do not propagate input capability information otherwise.
                self.local_pointstamp_internal.update((0, input, time.clone()), delta);
                // For each edge emanating from the input, indicate the existence of more records.
                for target in &self.children[0].edges[input] {
                    self.local_pointstamp_messages.update((target.index, target.port, time.clone()), delta);
                }
            }
        }

        // Step 2: Extract progress summaries from child subscopes.
        //
        // Each managed scope reports changes to pointstamp counts, both for its input and output record
        // counts, and for any changes to its internal capabilities. This information can either be "local"
        // in which case the data *must* be exchanged with peer subgraphs, or the information is "global"
        // in which case the data *must not* be exchanged with peer subgraphs (in this case, other peers
        // will receive the same data from their corresponding children).
        //
        // Updates are placed directly in the pointstamp accumulators.
        for child in &mut self.children {

            // if local, update into the local pointstamps, which must be exchanged. otherwise into
            // the global pointstamps, which need not be exchanged.
            let subactive = if child.local {
                child.pull_pointstamps(
                    &mut self.local_pointstamp_messages,
                    &mut self.local_pointstamp_internal,
                )
            }
            else {
                child.pull_pointstamps(
                    &mut self.final_pointstamp_messages,
                    &mut self.final_pointstamp_internal,
                )
            };

            // Any active child requires we remain active as well.
            active = active || subactive;
        }

        // Intermission: exchange pointstamp updates, then move them to the pointstamps structure.
        //
        // Note: Only "local" pointstamp updates should be exchanged. Any "final" pointstamp updates
        // should be directly installed without exchanging them.
        self.progcaster.send_and_recv(
            &mut self.local_pointstamp_messages,
            &mut self.local_pointstamp_internal,
        );

        // All local pointstamp *message* updates can be folded into the final updates.
        self.local_pointstamp_messages.drain_into(&mut self.final_pointstamp_messages);

        // For local pointstamp *internal* udpates, we must special case the `child == 0` case to
        // extract the changes as consumed messages, rather than altered capabilities.
        for ((index, port, timestamp), delta) in self.local_pointstamp_internal.drain() {
            if index == 0 {
                // Update our report upwards of consumed records.
                consumed[port].update(timestamp.outer.clone(), delta);
            }
            else {
                // Fold non-index_0 changes into the final updates.
                self.final_pointstamp_internal.update((index, port, timestamp), delta);
            }
        }

        // Step 3: React to post-exchange poinstamp updates.
        //
        // We now have a collection of pointstamp updates that should be applied. Before
        // alerting anyone, we push the updates through filters that report only changes
        // in the *discrete frontier* of timestamps at each location. This has the effect
        // of recording the changes, but only providing information to others when some
        // fundamental change in the frontier has occurred, suppressing variations in the
        // positive counts or any members dominated by those with positive counts.

        // Messages: de-multiplex changes first, then determine changes in consequences second.
        for ((index, input, time), delta) in self.final_pointstamp_messages.drain() {
            // Any change that heads to an output should be reported as produced.
            if index == 0 { produced[input].update(time.outer.clone(), delta); }
            self.children[index].messages[input].update_dirty(time, delta);
        }
        // Now propagate message updates through each filter, updating `self.pointstamps`
        // for any changes in the lower bound of elements with non-zero count.
        for index in 0 .. self.children.len() {
            let pointstamps = &mut self.pointstamps;
            for input in 0 .. self.children[index].messages.len() {
                self.children[index].messages[input].update_iter_and(None, |time, delta| 
                    pointstamps.update_target(Target { index: index, port: input }, time.clone(), delta)
                );
            }
        }

        // Internal: de-multiplex changes first, then determine changes in consequences second.
        for ((index, output, time), delta) in self.final_pointstamp_internal.drain() {
            self.children[index].internal[output].update_dirty(time, delta);
        }
        // Now propagate internal updates through each filter, updating `self.pointstamps`
        // for any changes in the lower bound of elements with non-zero count.
        for index in 0 .. self.children.len() {
            let pointstamps = &mut self.pointstamps;
            for output in 0 .. self.children[index].internal.len() {
                self.children[index].internal[output].update_iter_and(None, |time, delta| 
                    pointstamps.update_source(Source { index: index, port: output }, time.clone(), delta)
                );
            }
        }

        // Step 4: Propagate changes in the frontier of timestamp information at each location.
        //
        // At this point, `self.pointstamps` is loaded with changes at locations and must be
        // triggered to produce changes at destinations, which we now do.
        self.push_pointstamps();

        // Step 5: Inform each managed child scope of changes to their input frontiers.
        //
        // Propagated pointstamp changes may have altered the input frontier of each child.
        // We provide this information to each of them, handled by their wrapper.
        for child in self.children.iter_mut().skip(1) {
            let pointstamps = &mut self.pointstamps.pushed[child.index][..];
            child.push_pointstamps(pointstamps);
        }

        // Step 6: Record changes in input frontiers to output ports as changes in capabilities.
        //
        // Propagated pointstamp changes arriving at an output correspond to changes in held
        // capabilities. We push each of these through a filter to report any changes to the
        // discrete frontier upwards in the `internal` argument.
        for (output, pointstamps) in self.pointstamps.pushed[0].iter_mut().enumerate() {
            let iterator = pointstamps.drain().map(|(time, diff)| (time.outer, diff));
            self.output_capabilities[output].update_iter_and(iterator, |t, v| {
                internal[output].update(t.clone(), v);
            });
        }

        self.pointstamps.clear();

        // if there are outstanding messages or capabilities, we must insist on continuing to run
        for child in &self.children {
            // if child.messages.iter().any(|x| x.frontier().len() > 0) 
            // { println!("{:?}: child {}[{}] has outstanding messages", self.path, child.name, child.index); }
            active = active || child.messages.iter().any(|x| !x.is_empty());
            // if child.internal.iter().any(|x| x.frontier().len() > 0) 
            // { println!("{:?} child {}[{}] has outstanding capabilities", self.path, child.name, child.index); }
            active = active || child.internal.iter().any(|x| !x.is_empty());
        }

        active
    }
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
            self.logging.log(::timely_logging::Event::Operates(::timely_logging::OperatesEvent {
                id: identifier,
                addr: child_path,
                name: child.name().to_owned(),
            }));
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

        for (source, target) in self.edge_stash {
            // println!("  edge: {:?}", (source, target));
            self.children[source.index].edges[source.port].push(target);
        }

        let progcaster = Progcaster::new(allocator, &self.path, self.logging.clone());

        Subgraph {
            name: self.name,
            path: self.path,
            inputs: self.input_messages.len(),
            outputs: self.output_capabilities.len(),
            children: self.children,
            input_messages: self.input_messages,
            output_capabilities: self.output_capabilities,

            pointstamps:               Default::default(),
            local_pointstamp_messages: ChangeBatch::new(),
            local_pointstamp_internal: ChangeBatch::new(),
            final_pointstamp_messages: ChangeBatch::new(),
            final_pointstamp_internal: ChangeBatch::new(),
            progcaster:                progcaster,
        }
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Subgraph<TOuter, TInner> {

    // pushes pointstamps to scopes using self.*_summaries; clears pre-push counts.
    fn push_pointstamps(&mut self) {

        // push pointstamps from targets to targets.
        for index in 0..self.pointstamps.target.len() {
            for input in 0..self.pointstamps.target[index].len() {
                for (time, value) in self.pointstamps.target[index][input].drain() {
                    for &(target, ref antichain) in &self.children[index].target_target_summaries[input] {
                        for summary in antichain.elements().iter() {
                            if let Some(new_time) = summary.results_in(&time) {
                                self.pointstamps.pushed[target.index][target.port].update(new_time, value);
                            }
                        }
                    }
                }
            }
        }

        // push pointstamps from sources to targets.
        for index in 0..self.pointstamps.source.len() {
            for output in 0..self.pointstamps.source[index].len() {
                for (time, value) in self.pointstamps.source[index][output].drain() {
                    for &(target, ref antichain) in &self.children[index].source_target_summaries[output] {
                        for summary in antichain.elements().iter() {
                            if let Some(new_time) = summary.results_in(&time) {
                                self.pointstamps.pushed[target.index][target.port].update(new_time, value);
                            }
                        }
                    }
                }
            }
        }
    }

    // sets source_target_summaries and target_target_summaries based on the transitive closure of
    // the current contents of self.edges and self.target_source_summaries. If the latter does not
    // contain connections from scope outputs to scope inputs, the summary will be scope-internal.
    fn compute_summaries(&mut self) {

        // println!("computing summaries for {:?}!", self.path);
        
        // for child_index in 0..self.children.len() {
        //     println!("child[{}]: {}", child_index, self.children[child_index].name);
        //     for output in 0..self.children[child_index].outputs {
        //         println!("  output[{}] edges", output);
        //         for x in &self.children[child_index].edges[output] {
        //             println!("    {:?}", x);
        //         }
        //     }
        // }

        let mut additions = ::std::collections::VecDeque::<(Source, Target, Summary<TOuter::Summary, TInner::Summary>)>::new();

        // add all edges as initial summaries, using the default summary (should be a property of
        // the summary type, I think).
        for child_index in 0..self.children.len() {
            assert_eq!(child_index, self.children[child_index].index);
            for s_port in 0..self.children[child_index].outputs {
                let edges = self.children[child_index].edges[s_port].clone();
                for &target in &edges {
                    if try_to_add_summary(&mut self.children[child_index].source_target_summaries[s_port], target, Default::default()) {
                        additions.push_back((Source { index: child_index, port: s_port }, target, Default::default()));
                    }
                }
            }
        }

        // repeatedly expand the summaries, stopping when no further progress is made.
        while let Some((source, target, summary)) = additions.pop_front() {
            // we want to consider extensions from (target, t_port) to (source', s_port'), and on
            // to (target', t_port') using first self.target_source_summaries and then self.edges.
            let temp = self.children[target.index].target_source_summaries[target.port].clone();
            for &(new_source, ref new_summaries) in &temp {
                for new_summary in new_summaries.elements() {
                    if let Some(summary) = summary.followed_by(new_summary) {
                        let edges = self.children[new_source.index].edges[new_source.port].clone();
                        for &new_target in &edges {
                            if try_to_add_summary(&mut self.children[source.index].source_target_summaries[source.port], new_target, summary.clone()) {
                                additions.push_back((source, new_target, summary.clone()));
                            }
                        }
                    }
                }
            }
        }

        // from a complete self.source_target_summaries, we complete self.target_target_summaries
        // by combining self.target_source_summaries and self.source_target_summaries.
        for child_index in 0..self.children.len() {
            for input in 0..self.children[child_index].inputs {
                // each input can result in "itself", even without an edge
                try_to_add_summary(
                    &mut self.children[child_index].target_target_summaries[input],
                    Target { index: child_index, port: input},
                    Local(Default::default())
                );

                let temp1 = self.children[child_index].target_source_summaries[input].clone();
                for &(source, ref summaries) in &temp1 {
                    for summary in summaries.elements() {
                        let temp2 = self.children[source.index].source_target_summaries[source.port].clone();
                        for &(target, ref new_summaries) in &temp2 {
                            for new_summary in new_summaries.elements() {
                                if let Some(summary) = summary.followed_by(new_summary) {
                                    try_to_add_summary(&mut self.children[child_index].target_target_summaries[input], target, summary);
                                }
                            }
                        }
                    }
                }
            }
        }

        // filter down the summaries to not reference children with child.notify == false
        let notify = self.children.iter().map(|x| x.notify).collect::<Vec<_>>();
        for child_index in 0..self.children.len() {
            for input in 0..self.children[child_index].inputs {
                self.children[child_index].target_target_summaries[input].retain(|x| { notify[x.0.index] });
            }
            for output in 0..self.children[child_index].outputs {
                self.children[child_index].source_target_summaries[output].retain(|x| { notify[x.0.index] });
            }
        }

        // for child_index in 0..self.children.len() {
        //     println!("child[{}]: {}", child_index, self.children[child_index].name);
        //     for input in 0..self.children[child_index].inputs {
        //         println!("  input[{}]", input);
        //         for x in &self.children[child_index].target_target_summaries[input] {
        //             println!("    {:?}", x);
        //         }
        //     }
        //     for output in 0..self.children[child_index].outputs {
        //         println!("  output[{}]", output);
        //         for x in &self.children[child_index].source_target_summaries[output] {
        //             println!("    {:?}", x);
        //         }
        //     }
        // }
    }
}

fn try_to_add_summary<T: Eq, S: PartialOrder+Eq+Debug>(vector: &mut Vec<(T, Antichain<S>)>, target: T, summary: S) -> bool {
    for &mut (ref t, ref mut antichain) in vector.iter_mut() {
        if target.eq(t) { return antichain.insert(summary); }
    }
    vector.push((target, Antichain::from_elem(summary)));
    true
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

    // summaries from sources and targets of the operator to other sources and targets.
    source_target_summaries: Vec<Vec<(Target, Antichain<T::Summary>)>>,
    target_target_summaries: Vec<Vec<(Target, Antichain<T::Summary>)>>,
    target_source_summaries: Vec<Vec<(Source, Antichain<T::Summary>)>>,

    messages: Vec<MutableAntichain<T>>, // outstanding input messages
    internal: Vec<MutableAntichain<T>>, // output capabilities expressed by the operator
    external: Vec<MutableAntichain<T>>, // input capabilities expressed by outer scope

    consumed_buffer: Vec<ChangeBatch<T>>, // per-input: temp buffer used for pull_internal_progress.
    internal_buffer: Vec<ChangeBatch<T>>, // per-output: temp buffer used for pull_internal_progress.
    produced_buffer: Vec<ChangeBatch<T>>, // per-output: temp buffer used for pull_internal_progress.

    external_buffer: Vec<ChangeBatch<T>>, // per-input: temp buffer used for push_external_progress.

    logging: Logger,
}

impl<T: Timestamp> PerOperatorState<T> {

    fn add_input(&mut self) {
        self.inputs += 1;
        self.target_target_summaries.push(vec![]);
        self.target_source_summaries.push(vec![]);
        self.messages.push(Default::default());
        self.external.push(Default::default());
        self.external_buffer.push(ChangeBatch::new());
        self.consumed_buffer.push(ChangeBatch::new());
    }
    fn add_output(&mut self) {
        self.outputs += 1;
        self.edges.push(vec![]);
        self.internal.push(Default::default());
        self.internal_buffer.push(ChangeBatch::new());
        self.produced_buffer.push(ChangeBatch::new());
        self.source_target_summaries.push(vec![]);
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
            internal: Vec::new(),
            messages: Vec::new(),
            external: Vec::new(),
            external_buffer: Vec::new(),
            consumed_buffer: Vec::new(),
            internal_buffer: Vec::new(),
            produced_buffer: Vec::new(),
            target_target_summaries: Vec::new(),
            source_target_summaries: Vec::new(),
            target_source_summaries: Vec::new(),

            logging: logging,
        }
    }

    pub fn new(mut scope: Box<Operate<T>>, index: usize, mut _path: Vec<usize>, identifier: usize, logging: Logger) -> PerOperatorState<T> {

        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let (summary, mut work) = scope.get_internal_summary();

        assert_eq!(summary.len(), inputs);
        assert!(!summary.iter().any(|x| x.len() != outputs));

        let mut new_summary = vec![Vec::new(); inputs];
        for input in 0..inputs {
            for output in 0..outputs {
                for summary in summary[input][output].elements() {
                    try_to_add_summary(&mut new_summary[input], Source { index: index, port: output }, summary.clone());
                }
            }
        }

        let mut result = PerOperatorState {
            name:       scope.name(),
            operator:      Some(scope),
            index:      index,
            id:         identifier,
            local:      local,
            inputs:     inputs,
            outputs:    outputs,
            edges:      vec![vec![]; outputs],

            notify:     notify,

            internal: vec![Default::default(); outputs],
            messages: vec![Default::default(); inputs],

            external: vec![Default::default(); inputs],

            external_buffer: vec![ChangeBatch::new(); inputs],

            consumed_buffer: vec![ChangeBatch::new(); inputs],
            internal_buffer: vec![ChangeBatch::new(); outputs],
            produced_buffer: vec![ChangeBatch::new(); outputs],

            target_target_summaries: vec![vec![]; inputs],
            source_target_summaries: vec![vec![]; outputs],

            target_source_summaries:    new_summary,

            logging:                    logging,
        };

        for index in 0 .. work.len() {
            result.internal[index].update_iter(work[index].drain());
        }

        // if result.name == "Subgraph" {
        //     println!("{:?}", result.name);
        //     println!("{:?}", result.internal);
        //     println!();
        // }

        result
    }

    pub fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<T::Summary>>>) {
        let frontier = &mut self.external_buffer;
        if self.index > 0 && self.notify && !frontier.is_empty() && !frontier.iter_mut().any(|x| !x.is_empty()) {
            println!("initializing notifiable operator {}[{}] with inputs but no external capabilities", self.name, self.index);
        }
        self.operator.as_mut().map(|scope| scope.set_external_summary(summaries, frontier));
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
        for (input, updates) in external_progress.iter_mut().enumerate() {
            let buffer = &mut self.external_buffer[input];
            self.external[input].update_iter_and(updates.iter().cloned(), |time, val| { 
                buffer.update(time.clone(), val); 
            });
        }

        {
            let changes = &mut self.external_buffer;
            if changes.iter_mut().any(|ref mut c| !c.is_empty()) {
                self.logging.log(::timely_logging::Event::PushProgress(::timely_logging::PushProgressEvent {
                    op_id: self.id,
                }));
            }
            self.operator.as_mut().map(|x| x.push_external_progress(changes));
            if changes.iter_mut().any(|x| !x.is_empty()) {
                println!("changes not consumed by {:?}", self.name);
            }
            debug_assert!(!changes.iter_mut().any(|x| !x.is_empty()));
        }
    }

    pub fn pull_pointstamps(&mut self, pointstamp_messages: &mut ChangeBatch<(usize, usize, T)>,
                                       pointstamp_internal: &mut ChangeBatch<(usize, usize, T)>) -> bool {

        let active = {

            self.logging.log(::timely_logging::Event::Schedule(::timely_logging::ScheduleEvent {
                id: self.id, start_stop: ::timely_logging::StartStop::Start
            }));

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

            // TODO(andreal): for logging, do we want this always enabled?
            let did_work = 
                self.consumed_buffer.iter_mut().any(|cm| !cm.is_empty()) ||
                self.internal_buffer.iter_mut().any(|cm| !cm.is_empty()) ||
                self.produced_buffer.iter_mut().any(|cm| !cm.is_empty());

            self.logging.log(::timely_logging::Event::Schedule(::timely_logging::ScheduleEvent {
                id: self.id, start_stop: ::timely_logging::StartStop::Stop { activity: did_work }
            }));

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

        // shutting down if nothing left to do
        if self.operator.is_some() &&
           !active &&
           self.notify && // we don't track guarantees and capabilities for non-notify scopes. bug?
           self.external.iter().all(|x| x.is_empty()) &&
           self.internal.iter().all(|x| x.is_empty()) {
            //    println!("Shutting down {}", self.name);
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
