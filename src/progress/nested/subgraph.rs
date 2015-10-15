//! Implements `Operate` for a scoped collection of child operators.

use std::default::Default;
use std::fmt::Debug;

use std::mem;

use std::rc::Rc;
use std::cell::RefCell;
use timely_communication::Allocate;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, PathSummary, Operate};
use progress::nested::Source::{GraphInput, ChildOutput};
use progress::nested::Target::{GraphOutput, ChildInput};

use progress::nested::summary::Summary::{Local, Outer};
use progress::count_map::CountMap;

use progress::broadcast::Progcaster;
use progress::nested::summary::Summary;
use progress::nested::scope_wrapper::ChildWrapper;
use progress::nested::pointstamp_counter::PointstampCounter;
use progress::nested::product::Product;

// IMPORTANT : by convention, a child identifier of zero is used to indicate inputs and outputs of
// the Subgraph itself. An identifier greater than zero corresponds to an actual child, which can
// be found at position (id - 1) in the `children` field of the Subgraph.

pub type Source = (usize, usize);
pub type Target = (usize, usize);

pub struct Subgraph<TOuter:Timestamp, TInner:Timestamp> {
    /// A helpful name describing the subgraph.
    name: String,

    /// A sequence of integers identifying the location of the subgraph.
    path: Vec<usize>,

    /// An integer indentifying the location of the subgraph in its parent scope.
    index: usize,

    /// The number of inputs to the scope.
    inputs: usize,

    /// The number of outputs from the scope.
    outputs: usize,

    /// Connections from child outputs to other locations in the subgraph.
    edges: Vec<Vec<Vec<Target>>>,

    // maps from (child, output), (child, input) and (input) to respective Vec<(target, antichain)> lists
    // by convention, child == 0 -> parent scope, with the corresponding source summaries being external
    // to the scope.
    source_target_summaries: Vec<Vec<Vec<(Target, Antichain<Summary<TOuter::Summary, TInner::Summary>>)>>>,
    target_target_summaries: Vec<Vec<Vec<(Target, Antichain<Summary<TOuter::Summary, TInner::Summary>>)>>>,
    target_source_summaries: Vec<Vec<Vec<(Source, Antichain<Summary<TOuter::Summary, TInner::Summary>>)>>>,

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<ChildWrapper<Product<TOuter, TInner>>>,

    // shared state write to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<CountMap<Product<TOuter, TInner>>>>>,

    // something about staging to make pointstamp propagation easier. I'll probably remember as I
    // walk through all this again. ><
    pointstamps: PointstampCounter<Product<TOuter, TInner>>,

    // pointstamp messages to exchange. ultimately destined for `messages` or `internal`.
    local_pointstamp_messages: CountMap<(usize, usize, Product<TOuter, TInner>)>,
    local_pointstamp_internal: CountMap<(usize, usize, Product<TOuter, TInner>)>,
    global_pointstamp_messages: CountMap<(usize, usize, Product<TOuter, TInner>)>,
    global_pointstamp_internal: CountMap<(usize, usize, Product<TOuter, TInner>)>,

    progcaster: Progcaster<Product<TOuter, TInner>>,

    // accumulated counts for (scope, port), where scope == 0 -> parent.
    // `messages` corresponds to inputs, and `internal` corresponds to outputs.
    // perhaps they could have names like `source_counts` and `target_counts` or something.
    messages: Vec<Vec<MutableAntichain<Product<TOuter, TInner>>>>,
    internal: Vec<Vec<MutableAntichain<Product<TOuter, TInner>>>>,

    // buffers for communication between parent and child.
    // these could possibly be elided if the methods were fused and updates written directly to
    // their destination buffers. maybe later.
    external_consumed_buffer: Vec<CountMap<TOuter>>,
    external_external_buffer: Vec<CountMap<TOuter>>,
    external_produced_buffer: Vec<CountMap<TOuter>>,
    internal_consumed_buffer: Vec<CountMap<TOuter>>,
    internal_internal_buffer: Vec<CountMap<TOuter>>,
    internal_produced_buffer: Vec<CountMap<TOuter>>,
}


impl<TOuter: Timestamp, TInner: Timestamp> Operate<TOuter> for Subgraph<TOuter, TInner> {

    fn name(&self) -> String { self.name.clone() }
    fn local(&self) -> bool { false }
    fn inputs(&self)  -> usize { self.inputs }
    fn outputs(&self) -> usize { self.outputs }

    // produces connectivity summaries from inputs to outputs, and reports initial internal
    // capabilities on each of the outputs (projecting capabilities from contained scopes).
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Vec<CountMap<TOuter>>) {

        // at this point, the subgraph is frozen. we should initialize any internal state which
        // may have been determined after construction (e.g. the numbers of inputs and outputs).
        // we also need to determine what to return as a summary and initial capabilities, which
        // will depend on child summaries and capabilities, as well as edges in the subgraph.

        // initially, inputs reach nothing and outputs cannot return to inputs.
        // but, we must have this here to avoid exposions.
        self.target_source_summaries.push(vec![Vec::new(); self.outputs()]);

        // initialize storage for input and output pointstamps
        self.pointstamps.target_pushed.push(vec![Default::default(); self.inputs()]);
        self.pointstamps.target_counts.push(vec![Default::default(); self.inputs()]);
        self.pointstamps.source_counts.push(vec![Default::default(); self.outputs()]);

        // seal subscopes; prepare per-scope state/buffers
        for child in &self.children {

            // incorporate the child summary into the scope's target-to-source summaries.
            self.target_source_summaries.push(child.summary.clone());

            // initialize storage for vector-based source and target path summaries.
            self.pointstamps.target_pushed.push(vec![Default::default(); child.inputs]);
            self.pointstamps.target_counts.push(vec![Default::default(); child.inputs]);
            self.pointstamps.source_counts.push(vec![Default::default(); child.outputs]);

            // introduce capabilities as pre-pushed pointstamps; will push to outputs.
            for output in (0..child.outputs) {
                for time in child.capabilities[output].elements().iter(){
                    self.pointstamp_internal.update(&(child.index, output, time.clone()), 1);
                }
            }
        }

        // determine summaries based on an empty `self.target_source_summary[0]`.
        // these will be strictly internal, which is what the method needs to return.
        self.set_summaries();

        // we must also push pointstamps to outputs, to determine initial capabilities expressed.
        self.push_pointstamps();

        // the initial capabilities should now be in `self.pointstamps.target_pushed[0]`, except
        // that they are `Product<TOuter, TInner>`, and we just want `TOuter`.
        let mut work = vec![CountMap::new(); self.outputs()];
        for (o_port, capabilities) in &self.pointstamps.target_pushed[0] {
            for &(time, val) in &capabilities.elements() {
                // make a note to self and inform scope of our capability.
                self.external_capability[output].update(&time.outer, val);
                work[o_port].update(&time.outer, val);
            }
        }
        // done with the pointstamps, so we should clean up.
        self.pointstamps.clear_pushed();

        // summarize the scope internals by looking for source_target_summaries from id 0 to id 0.
        let mut summaries = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];
        for input in (0..self.inputs()) {
            for &((target, output), ref antichain) in self.source_target_summary[0][input].iter() {
                if target == 0 {
                    for &summary in antichain.elements().iter() {
                        summaries[input][output].insert(match summary {
                            Local(_)    => Default::default(),
                            Outer(y, _) => y,
                        });
                    };
                }
            }
        }

        return (summaries, work);
    }

    // receives connectivity summaries from outputs to inputs, as well as initial external
    // capabilities on inputs. prepares internal summaries, and recursively calls the method on
    // contained scopes.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<TOuter::Summary>>>, frontier: &mut [CountMap<TOuter>]) {

        // we must now finish the work of setting up the subgraph, using the external summary and
        // external capabilities on the subgraph's inputs.
        for output in 0..self.outputs {
            for input in 0..self.inputs {
                for &summary in &summaries[output][input].elements() {
                    try_to_add_summary(&mut self.target_source_summaries[0][output], (0, input), Outer(summary, Default::default()));
                }
            }
        }

        // now re-compute all summaries with `self.target_source_summaries[0]` set.
        self.set_summaries();


        // initialize pointstamps for capabilities, from `frontier` and from child capabitilies.
        // change frontier to local times; introduce as pointstamps
        for input in 0..self.inputs {
            while let Some((time, val)) = frontier[input].pop() {
                self.pointstamps.update_source((0, input), &Product::new(time, Default::default()), val);
            }
        }

        // identify all capabilities expressed locally
        for child in &self.children {
            for output in 0..child.outputs {
                for time in child.capabilities[output].elements().iter() {
                    self.pointstamps.update_source((child.index, output), time, 1);
                }
            }
        }

        self.push_pointstamps();

        // we now have summaries and pushed pointstamps, which should be sufficient to recursively
        // call `set_external_summary` for each of the children.

        for child in &mut self.children {

            // set the initial capabilities based on the contents of self.pointstamps.target_pushed[child.index]
            for input in 0..child.inputs {
                child.update_input_capability()
            }

            // summarize the subgraph by the path summaries from the child's output to its inputs.
            let mut summary = vec![vec![Antichain::new(); child.inputs]; child.outputs];
            for output in 0..child.outputs {
                for &((source, s_port), ref antichain) in &self.source_target_summaries[child.index][output] {
                    if source == child.index {
                        summary[output][s_port] = antichain.clone();
                    }
                }
            }

            self.children.set_external_summary(summary, changes);
        }

        // clean up after ourselves.
        self.pointstamps.clear_pushed();
    }

    // information about the outside world, specifically messages consumed in the output, capabilities
    // held elsewhere in the graph, and messages produced on the inputs. It isn't super clear what
    // the consumed information is supposed to tell us at this point, but it is here for symmetry.
    fn push_external_progress(&mut self, external: &mut [CountMap<TOuter>]) {
        // delay processing this until the `step` method is called.
        for (input, external) in external.iter_mut().enumerate() {
            external.drain_into(&mut self.external_external_buffer[output]);
        }
    }

    // information from the vertex about its progress (updates to the output frontiers, recv'd and sent message counts)
    // the results of this method are meant to reflect state *internal* to the local instance of this
    // scope; although the scope may exchange progress information to determine whether something is runnable,
    // the exchanged information should probably *not* be what is reported back. only the locally
    // produced/consumed messages, and local internal work should be reported back up.
    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<TOuter>],
                                         internal: &mut [CountMap<TOuter>],
                                         produced: &mut [CountMap<TOuter>]) -> bool {

        // read from self.external_*_buffer, write to self.internal_*_buffer, perform any progress
        // logic, computation (as appropriate; perhaps this should be elsewhere), and indicate if
        // we need to continue execution despite apparent completion.
        let result = self.step();

        for (output, c) in self.internal_consumed_buffer.iter_mut().enumerate() {
            c.drain_into(&mut consumed[output]);
        }

        for (input, e) in self.internal_internal_buffer.iter_mut().enumerate() {
            e.drain_into(&mut internal[output]);
        }

        for (input, p) in self.internal_produced_buffer.iter_mut().enumerate() {
            p.drain_into(&mut produced[output]);
        }

        return result;
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Subgraph<TOuter, TInner> {

    /// Performs computation for the subgraph.
    ///
    /// This is isolated from the progress call and return sites in the interest of clarity and/or
    /// compartmentalization of effects.
    pub fn step(&mut self) -> bool {

        // should be false when there is nothing left to do
        let mut active = false;

        // Step 1: Observe the number of records this instances has accepted as input.
        //
        // This information should be exchanged with peers, probably as subtractions to the output
        // "capabilities" of the input ports. This is a bit non-standard, but ... it makes sense.
        // The "resulting" increments to message counts should also be exchanged at the same time.
        for input in (0..self.inputs) {
            let mut borrowed = self.input_messages[input].borrow_mut();
            while let Some((time, delta)) = borrowed.pop() {
                self.pointstamp_internal.update(&(0, input, time), -delta);
                for &(index, port) in self.edges[0][input].iter() {
                    self.pointstamp_messages.update(&(index, port, time), delta);
                }
            }
        }

        // Step 2: pull_internal_progress from subscopes.
        //
        // Updates that are local to this worker must be shared with other workers, whereas updates
        // that have already been shared with other workers should *not* be shared.

        for child in self.children.iter_mut() {

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
                    &mut self.global_pointstamp_messages,
                    &mut self.global_pointstamp_internal,
                )
            }

            active = active || subactive;
        }

        // Intermission: exchange pointstamp updates, and then move them to the pointstamps structure.
        //
        // Note : Not all pointstamps should be exchanged. Some should be immediately installed if
        // they correspond to scopes that are not .local(). These scopes represent operators that
        // have already exchanged their progress information, and whose messages already reflect
        // full information.

        // exchange progress messages that need to be exchanged.
        self.progcaster.send_and_recv(
            &mut self.local_pointstamp_messages,
            &mut self.local_pointstamp_internal,
        );

        // at this point we may need to do something horrible and filthy.
        // we have exchanged "capabilities" from the subgraph inputs, which we used to indicate
        // messages received by this instance. They are not capabilities, and should not be pushed
        // as pointstamps. Rather the contents of `self.external_external_buffer` should be pushed
        // as pointstamps. So, at this point it may be important to carefully remove elements of
        // `self.local_pointstamp_internal` with index 0, and introduce elements from `external_*`.
        // at least, the `drain_into` we are about to do should perform different logic as needed.

        // fold exchanged messages into the view of global progress.
        self.local_pointstamp_messages.drain_into(&mut self.global_pointstamp_messages);
        while let Some(((index, port, timestamp), delta)) = self.local_pointstamp_internal.pop() {
            if index == 0 { self.internal_consumed_buffer[port].update(&timestamp, delta); }
            else { self.global_pointstamp_internal.update(&(index, port, timestamp), delta); }
        }

        for input in 0..self.inputs {
            while let Some((time, val)) = self.external_external_buffer[input].pop() {
                self.global_pointstamp_internal.update(&(0, input, time), val)
            }
        }

        // Having exchanged progress updates, push message and capability updates through a filter
        // which indicates changes to the discrete frontier of the the antichain. This suppresses
        // changes that do not change the discrete frontier, even if they change counts or add new
        // elements beyond the frontier.
        //
        // We must also inform each child of changes to the number of inbound messages.
        while let Some(((scope, input, time), delta)) = self.global_pointstamp_messages.pop() {
            let pointstamps = &mut self.pointstamps;
            self.messages[scope][input].update_and(&time, delta, |time, delta|
                pointstamps.update_target(scope, input, time, delta)
            );
        }
        while let Some(((scope, output, time), delta)) = self.global_pointstamp_internal.pop() {
            let pointstamps = &mut self.pointstamps;
            self.internal[scope][output].update_and(&time, delta, |time, delta|
                pointstamps.update_source(ChildOutput(scope, output), time, delta)
            );
        }

        // push implications of outstanding work at each location to other locations in the graph.
        self.push_pointstamps();

        // update input capabilities for children
        for child in self.children.iter_mut() {
            let index = child.index();
            child.push_pointstamps(&self.pointstamps.target_pushed[index][..]);
        }

        for output in 0..self.outputs {
            while let Some((time, val)) = self.pointstamps.target_pushed[0][output].pop() {
                self.external_capability[output].update_and(&time.outer, val, |t,v|
                    self.internal_internal_buffer[output].update(t, v);
                );
            }
        }

        self.pointstamps.clear();

        // if there are outstanding messages or capabilities, we must insist on continuing to run
        for child in self.children.iter() {
            active = active || child.outstanding_messages.iter().any(|x| x.elements().len() > 0);
            active = active || child.capabilities.iter().any(|x| x.elements().len() > 0);
        }

        return active;
    }

    // pushes pointstamps to scopes using self.*_summaries; clears pre-push counts.
    fn push_pointstamps_to_scopes(&mut self) -> () {
        // push pointstamps from targets to targets.
        for index in 0..self.pointstamps.target_counts.len() {
            for input in (0..self.pointstamps.target_counts[index].len()) {
                while let Some((time, value)) = self.pointstamps.target_counts[index][input].pop() {
                    for &((scope, target_input), ref antichain) in self.target_target_summaries[index][input].iter() {
                        for summary in antichain.elements().iter() {
                            self.pointstamps.target_pushed[scope, target_input].update(&summary.results_in(&time), value);
                        }
                    }
                }
            }
        }

        // push pointstamps from sources to targets.
        for index in 0..self.pointstamps.source_counts.len() {
            for output in (0..self.pointstamps.source_counts[index].len()) {
                while let Some((time, value)) = self.pointstamps.source_counts[index][input].pop() {
                    for &((scope, target_input), ref antichain) in self.source_target_summaries[index][output].iter() {
                        for summary in antichain.elements().iter() {
                            self.pointstamps.target_pushed[scope, target_input].update(&summary.results_in(&time), value);
                        }
                    }
                }
            }
        }
    }

    // sets source_target_summaries and target_target_summaries based on the transitive closure of
    // the current contents of self.edges and self.target_source_summaries. If the latter does not
    // contain connections from scope outputs to scope inputs, the summary will be scope-internal.
    fn set_summaries(&mut self) {

        let mut additions = std::collections::VecDeque::new();

        // add all edges as initial summaries, using the default summary (should be a property of
        // the summary type, I think).
        for scope in 0..self.edges.len() {
            for s_port in 0..self.edges[scope].len() {
                for &(target, t_port) in &self.edges[scope][s_port] {
                    additions.push(((source, s_port), (target, t_port), Antichain::from_elem(self.default_summary)));
                }
            }
        }

        // repeatedly expand the summaries, stopping when no further progress is made.
        while let Some(((source, s_port), (target, t_port), summary)) = additions.pop_front() {
            // we want to consider extensions from (target, t_port) to (source', s_port'), and on
            // to (target', t_port') using first self.target_source_summaries and then self.edges.
            for &((new_source, new_s_port), ref new_summary) in &self.target_source_summaries[target][t_port] {
                let summary = summary.follow_by(new_summary);
                for &(new_target, new_t_port) in &self.edges[new_source][new_s_port] {
                    if try_to_add_summary(&mut self.source_target_summaries[source][s_port], (new_target, new_t_port), summary) {
                        additions.push_back(((source, s_port), (new_target, new_t_port), summary));
                    }
                }
            }
        }

        // from a complete self.source_target_summaries, we complete self.target_target_summaries
        // by combining self.target_source_summaries and self.source_target_summaries.
        for target in 0..self.target_source_summaries.len() {
            for t_port in 0..self.target_source_summarien[target].len() {
                for &((source, s_port), ref summary) in &self.target_source_summaries[target][t_port] {
                    for &((new_target, new_t_port), ref new_summary) in &self.source_target_summaries[source][s_port] {
                        let summary = summary.followed_by(new_summary);
                        try_to_add_summary(&mut self.target_target_summaries[target][t_port], (new_target, new_t_port), summary);
                    }
                }
            }
        }
    }

    pub fn new_input(&mut self, shared_counts: Rc<RefCell<CountMap<Product<TOuter, TInner>>>>) -> usize {
        self.inputs += 1;
        self.external_guarantee.push(MutableAntichain::new());
        self.input_messages.push(shared_counts);
        self.input_edges.push(Vec::new());
        return self.inputs - 1;
    }

    pub fn new_output(&mut self) -> usize {
        self.outputs += 1;
        self.external_capability.push(MutableAntichain::new());
        return self.outputs - 1;
    }

    pub fn connect(&mut self, source: Source, target: Target) {
        match source {
            ChildOutput(scope, index) => { self.children[scope].add_edge(index, target); },
            GraphInput(input) => {
                while (self.input_edges.len()) < (input + 1) { self.input_edges.push(Vec::new()); }
                self.input_edges[input].push(target);
            },
        }
    }

    pub fn new_from<A: Allocate>(allocator: &mut A, index: usize, path: Vec<usize>) -> Subgraph<TOuter, TInner> {
        let progcaster = Progcaster::new(allocator);
        let mut path = path;
        path.push(index);
        Subgraph {
            name:                   format!("Subgraph"),
            path:                   path,
            index:                  index,
            default_summary:        Default::default(),
            inputs:                 Default::default(),
            outputs:                Default::default(),
            input_edges:            Default::default(),
            external_summaries:     Default::default(),
            source_summaries:       Default::default(),
            target_summaries:       Default::default(),
            input_summaries:        Default::default(),
            external_capability:    Default::default(),
            external_guarantee:     Default::default(),
            children:               Default::default(),
            input_messages:         Default::default(),
            pointstamps:            Default::default(),
            pointstamp_messages:    Default::default(),
            pointstamp_internal:    Default::default(),
            progcaster:             progcaster,
        }
    }

    fn _print_topology(&self) {
        println!("Topology for {:?}", self.path);

        for child in self.children.iter() {
            for (output, list) in child.edges.iter().enumerate() {
                for target in list {
                    match *target {
                        ChildInput(index, port) => {
                            println!("  {}({}) -> {}({})", child.name(), output, self.children[index].name(), port);
                        },
                        GraphOutput(port) => {
                            println!("  {}({}) -> SELF({})", child.name(), output, port);
                        }
                    }
                }
            }
        }

        for (input, list) in self.input_edges.iter().enumerate() {
            for target in list {
                println!("  SELF({}) -> {:?}", input, target);
            }
        }
    }

    fn _print_reachability_summaries(&self) {
        println!("Reachability summary for subscope {:?}", self.path);
        println!("Operate outputs -> targets:");

        for i in 0..self.source_summaries.len() {
            for j in 0..self.source_summaries[i].len() {
                println!("\t{}.{} {}(output[{}]) ->", i, j, self.children[i].name(), j);
                for &(ref target, ref chain) in self.source_summaries[i][j].iter() {
                    println!("\t\t{}:\t{:?}", match target {
                        &ChildInput(scope, input) => format!("{}[{}](input[{}])", self.children[scope as usize].name(), scope, input),
                        x => format!("{:?} ", x),
                    }, chain.elements());
                }
            }
        }

        println!("Operate inputs -> targets:");
        for i in 0..self.target_summaries.len() {
            for j in 0..self.target_summaries[i].len() {
                println!("\t{}.{} {}(input[{}]) ->", i, j, self.children[i].name(), j);
                for &(ref target, ref chain) in self.target_summaries[i][j].iter() {
                    println!("\t\t{}:\t{:?}", match target {
                        &ChildInput(scope, input) => format!("{}[{}](input[{}])", self.children[scope as usize].name(), scope, input),
                        x => format!("{:?} ", x),
                        }, chain.elements());
                }
            }
        }
    }

    fn _print_status(&self) {
        println!("printing status for {:?}", self.path);
        for child in self.children.iter() {
            for (index, messages) in child.outstanding_messages.iter().enumerate() {
                if messages.elements().len() > 0 {
                    println!("  ({:?})::{}.messages[{}]: {:?}", self.path, child.name(), index, messages.elements());
                }
            }

            for (index, internal) in child.capabilities.iter().enumerate() {
                if internal.elements().len() > 0 {
                    println!("  ({:?})::{}.internal[{}]: {:?}", self.path, child.name(), index, internal.elements());
                }
            }
        }
    }
}

fn try_to_add_summary<S: PartialOrd+Eq+Copy+Debug>(vector: &mut Vec<(Target, Antichain<S>)>, target: Target, summary: S) -> bool {
    for &mut (ref t, ref mut antichain) in vector.iter_mut() {
        if target.eq(t) { return antichain.insert(summary); }
    }
    vector.push((target, Antichain::from_elem(summary)));
    return true;
}
