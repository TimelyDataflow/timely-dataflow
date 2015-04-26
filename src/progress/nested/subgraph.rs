use std::default::Default;
use std::fmt::Debug;

use std::mem;

use std::rc::Rc;
use std::cell::RefCell;
use communication::Communicator;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, PathSummary, Scope};
use progress::nested::Source::{GraphInput, ScopeOutput};
use progress::nested::Target::{GraphOutput, ScopeInput};

use progress::nested::summary::Summary::{Local, Outer};
use progress::count_map::CountMap;

use progress::broadcast::{Progcaster, ProgressVec};
use progress::nested::summary::Summary;
use progress::nested::scope_wrapper::ScopeWrapper;
use progress::nested::pointstamp_counter::PointstampCounter;
use progress::nested::product::Product;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug)]
pub enum Source {
    GraphInput(u64),        // from outer scope
    ScopeOutput(u64, u64),  // (scope, port) may have interesting connectivity
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug)]
pub enum Target {
    GraphOutput(u64),       // to outer scope
    ScopeInput(u64, u64),   // (scope, port) may have interesting connectivity
}

pub struct Subgraph<TOuter:Timestamp, TInner:Timestamp> {
    pub name:               String,                     // a helpful name
    pub index:              u64,                        // a useful integer

    default_summary:        Summary<TOuter::Summary, TInner::Summary>,    // default summary to use for something TODO: figure out what.

    pub inputs:                 u64,                        // number inputs into the scope
    pub outputs:                u64,                        // number outputs from the scope

    input_edges:            Vec<Vec<Target>>,           // edges as list of Targets for each input_port.

    external_summaries:     Vec<Vec<Antichain<TOuter::Summary>>>,// path summaries from output -> input (TODO: Check) using any edges

    // maps from (scope, output), (scope, input) and (input) to respective Vec<(target, antichain)> lists
    // TODO: sparsify complete_summaries to contain only paths which avoid their target scopes.
    // TODO: differentiate summaries by type of destination, to remove match from inner-most loop (of push_poinstamps).
    source_summaries:       Vec<Vec<Vec<(Target, Antichain<Summary<TOuter::Summary, TInner::Summary>>)>>>,
    target_summaries:       Vec<Vec<Vec<(Target, Antichain<Summary<TOuter::Summary, TInner::Summary>>)>>>,
    input_summaries:        Vec<Vec<(Target, Antichain<Summary<TOuter::Summary, TInner::Summary>>)>>,

    // state reflecting work in and promises made to external scope.
    pub external_capability:    Vec<MutableAntichain<TOuter>>,
    pub external_guarantee:     Vec<MutableAntichain<TOuter>>,

    pub children:               Vec<ScopeWrapper<Product<TOuter, TInner>>>,

    pub input_messages:         Vec<Rc<RefCell<CountMap<Product<TOuter, TInner>>>>>,

    pointstamps:            PointstampCounter<Product<TOuter, TInner>>,

    pointstamp_messages_cm: CountMap<(u64, u64, Product<TOuter, TInner>)>,
    pointstamp_internal_cm: CountMap<(u64, u64, Product<TOuter, TInner>)>,
    pointstamp_messages:    ProgressVec<Product<TOuter, TInner>>,
    pointstamp_internal:    ProgressVec<Product<TOuter, TInner>>,

    progcaster:             Progcaster<Product<TOuter, TInner>>,
}


impl<TOuter: Timestamp, TInner: Timestamp> Scope<TOuter> for Subgraph<TOuter, TInner> {
    fn name(&self) -> String { self.name.clone() }
    fn inputs(&self)  -> u64 { self.inputs }
    fn outputs(&self) -> u64 { self.outputs }

    // produces (in -> out) summaries using only edges internal to the vertex.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Vec<CountMap<TOuter>>) {
        // seal subscopes; prepare per-scope state/buffers
        for index in (0..self.children.len()) {
            let inputs  = self.children[index].inputs as usize;
            let outputs = self.children[index].outputs as usize;

            // initialize storage for vector-based source and target path summaries.
            self.source_summaries.push(vec![Vec::new(); outputs]);
            self.target_summaries.push(vec![Vec::new(); inputs]);

            self.pointstamps.target_pushed.push(vec![Default::default(); inputs]);
            self.pointstamps.target_counts.push(vec![Default::default(); inputs]);
            self.pointstamps.source_counts.push(vec![Default::default(); outputs]);

            // take capabilities as pointstamps
            for output in (0..outputs) {
                for time in self.children[index].capabilities[output].elements.iter(){
                    self.pointstamps.update_source(ScopeOutput(index as u64, output as u64), time, 1);
                }
            }
        }

        // initialize space for input -> Vec<(Target, Antichain) mapping.
        self.input_summaries = vec![Vec::new(); self.inputs() as usize];

        self.pointstamps.input_counts = vec![Default::default(); self.inputs() as usize];
        self.pointstamps.output_pushed = vec![Default::default(); self.outputs() as usize];

        self.external_summaries = vec![vec![Default::default(); self.inputs() as usize]; self.outputs() as usize];

        // TODO: Explain better.
        self.set_summaries();

        self.push_pointstamps_to_targets();

        // TODO: WTF is this all about? Who wrote this? Me...
        let mut work = vec![CountMap::new(); self.outputs() as usize];
        for (output, map) in work.iter_mut().enumerate() {
            for &(ref key, val) in self.pointstamps.output_pushed[output].elements().iter() {
                map.update(&key.outer, val);
                self.external_capability[output].update(&key.outer, val);
            }
        }

        let mut summaries = vec![vec![Antichain::new(); self.outputs() as usize]; self.inputs() as usize];

        for input in (0..self.inputs()) {
            for &(target, ref antichain) in self.input_summaries[input as usize].iter() {
                if let GraphOutput(output) = target {
                    for &summary in antichain.elements.iter() {
                        summaries[input as usize][output as usize].insert(match summary {
                            Local(_)    => Default::default(),
                            Outer(y, _) => y,
                        });
                    };
                }
            }
        }

        self.pointstamps.clear_pushed();

        return (summaries, work);
    }

    // receives (out -> in) summaries using only edges external to the vertex.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<TOuter::Summary>>>, frontier: &mut [CountMap<TOuter>]) -> () {
        self.external_summaries = summaries;
        self.set_summaries();

        // self.print_reachability_summaries();

        // change frontier to local times; introduce as pointstamps
        for graph_input in (0..self.inputs) {
            while let Some((time, val)) = frontier[graph_input as usize].pop() {
                self.pointstamps.update_source(GraphInput(graph_input), &Product::new(time, Default::default()), val);
            }
        }

        // identify all capabilities expressed locally
        for scope in (0..self.children.len()) {
            for output in (0..self.children[scope].outputs) {
                for time in self.children[scope].capabilities[output as usize].elements.iter() {
                    self.pointstamps.update_source(ScopeOutput(scope as u64, output), time, 1);
                }
            }
        }

        self.push_pointstamps_to_targets();

        // for each subgraph, compute summaries based on external edges.
        for subscope in (0..self.children.len()) {
            let mut changes = mem::replace(&mut self.children[subscope].guarantee_changes, Vec::new());

            if self.children[subscope].notify {
                for input_port in (0..changes.len()) {
                    self.children[subscope]
                        .guarantees[input_port]
                        .update_into_cm(&self.pointstamps.target_pushed[subscope][input_port], &mut changes[input_port]);
                }
            }

            let inputs = self.children[subscope].inputs as usize;
            let outputs = self.children[subscope].outputs as usize;

            let mut summaries = vec![vec![Antichain::new(); inputs]; outputs];

            for output in (0..summaries.len()) {
                for &(target, ref antichain) in self.source_summaries[subscope][output].iter() {
                    if let ScopeInput(target_scope, target_input) = target {
                        if target_scope == subscope as u64 { summaries[output][target_input as usize] = antichain.clone() }
                    }
                }
            }

            self.children[subscope].set_external_summary(summaries, &mut changes);

            // TODO : Shouldn't be necesasary ...
            if changes.iter().any(|x| x.len() > 0) {
                println!("Someone ({}) didn't take their medicine...", self.children[subscope].name());
                for change in changes.iter_mut() { change.clear(); }
            }

            mem::replace(&mut self.children[subscope].guarantee_changes, changes);
        }

        self.pointstamps.clear_pushed();
    }

    // information for the scope about progress in the outside world (updates to the input frontiers)
    // important to push this information on to subscopes.
    fn push_external_progress(&mut self, external_progress: &mut [CountMap<TOuter>]) -> () {
        // transform into pointstamps to use push_progress_to_target().
        for (input, progress) in external_progress.iter_mut().enumerate() {
            while let Some((time, val)) = progress.pop() {
                self.pointstamps.update_source(GraphInput(input as u64), &Product::new(time, Default::default()), val);
            }
        }

        self.push_pointstamps_to_targets();

        // consider pushing to each nested scope in turn.
        for (index, child) in self.children.iter_mut().enumerate() {
            child.push_pointstamps(&self.pointstamps.target_pushed[index]);
        }

        self.pointstamps.clear_pushed();
    }

    // information from the vertex about its progress (updates to the output frontiers, recv'd and sent message counts)
    fn pull_internal_progress(&mut self, internal_progress: &mut [CountMap<TOuter>],
                                         messages_consumed: &mut [CountMap<TOuter>],
                                         messages_produced: &mut [CountMap<TOuter>]) -> bool {
        // should be false when there is nothing left to do
        let mut active = false;

        // Step 1: handle messages introduced through each graph input
        for input in (0..self.inputs) {
            while let Some((time, delta)) = self.input_messages[input as usize].borrow_mut().pop() {
                messages_consumed[input as usize].update(&time.outer, delta);
                for &target in self.input_edges[input as usize].iter() {
                    match target {
                        ScopeInput(tgt, tgt_in)   => { self.pointstamp_messages.push((tgt, tgt_in, time, delta)); },
                        GraphOutput(graph_output) => { messages_produced[graph_output as usize].update(&time.outer, delta); },
                    }
                }
            }
        }

        // Step 2: pull_internal_progress from subscopes.
        for child in self.children.iter_mut() {
            let subactive = child.pull_pointstamps(&mut self.pointstamp_messages,
                                                   &mut self.pointstamp_internal,
                                                   |out, time, delta| { messages_produced[out as usize].update(&time.outer, delta); });

            if subactive {
                active = true;
                // println!("{}: not done because child {} said so", self.name, child.scope.name());
            }
        }

        // Intermission: exchange pointstamp updates, and then move them to the pointstamps structure.
        self.progcaster.send_and_recv(&mut self.pointstamp_messages, &mut self.pointstamp_internal);

        {
            // this aggregates down the pointstamps, removing any cancelled updates
            while let Some((a, b, c, d)) = self.pointstamp_messages.pop() { self.pointstamp_messages_cm.update(&(a, b, c), d); }
            while let Some(((a, b, c), d)) = self.pointstamp_messages_cm.pop() { self.pointstamp_messages.push((a, b, c, d)); }
            while let Some((a, b, c, d)) = self.pointstamp_internal.pop() { self.pointstamp_internal_cm.update(&(a, b, c), d); }
            while let Some(((a, b, c), d)) = self.pointstamp_internal_cm.pop() { self.pointstamp_internal.push((a, b, c, d)); }

            let pointstamps = &mut self.pointstamps;    // clarify to Rust that we don't need &mut self for the closures.
            // println!("ps_msg: {:?}", self.pointstamp_messages);
            for (scope, input, time, delta) in self.pointstamp_messages.drain() {
                // println!("  {}  {:?}  {:?}  {:?}", self.children[scope as usize].scope.name(), input, time, delta);
                self.children[scope as usize].outstanding_messages[input as usize].update_and(&time, delta, |time, delta| {
                    // println!("    update passed: {:?} {}", time, delta);
                    pointstamps.update_target(ScopeInput(scope, input), time, delta);
                });
            }

            // println!("ps_int: {:?}", self.pointstamp_internal);
            for (scope, output, time, delta) in self.pointstamp_internal.drain() {
                self.children[scope as usize].capabilities[output as usize].update_and(&time, delta, |time, delta| {
                    pointstamps.update_source(ScopeOutput(scope, output), time, delta);
                });
            }
        }

        self.push_pointstamps_to_targets();     // moves self.pointstamps to self.pointstamps.pushed, differentiated by target.

        // Step 3: push any progress to each target subgraph ...
        for (index, child) in self.children.iter_mut().enumerate() {
            child.push_pointstamps(&self.pointstamps.target_pushed[index]);
        }

        // Step 4: push progress to each graph output ...
        for output in (0..self.outputs) {
            while let Some((time, val)) = self.pointstamps.output_pushed[output as usize].pop() {
                // println!("Output pushed: {:?} {}", time, val);
                self.external_capability[output as usize].update_and(&time.outer, val, |t,v| {
                    internal_progress[output as usize].update(t, v);
                });
            }
        }

        // println!("returning with internal_progress: {:?}", internal_progress);
        // println!("               messages_consumed: {:?}", messages_consumed);
        // println!("               messages_produced: {:?}", messages_produced);

        // pointstamps should be cleared in push_to_targets()
        self.pointstamps.clear_pushed();

        for child in self.children.iter() {
            active = active || child.outstanding_messages.iter().any(|x| x.elements.len() > 0);
            active = active || child.capabilities.iter().any(|x| x.elements.len() > 0);
        }

        // if we want to see why we are active
        // if active { self.print_status(); }

        return active;
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Subgraph<TOuter, TInner> {
    pub fn children(&self) -> usize { self.children.len() }

    fn push_pointstamps_to_targets(&mut self) -> () {
        for index in (0..self.children.len()) {
            for input in (0..self.pointstamps.target_counts[index].len()) {
                while let Some((time, value)) = self.pointstamps.target_counts[index][input as usize].pop() {
                    // println!("propagating: {}[{}]: {}", self.children[index].scope.name(), input, value);
                    for &(target, ref antichain) in self.target_summaries[index][input as usize].iter() {
                        let mut dest = match target {
                            ScopeInput(scope, input) => &mut self.pointstamps.target_pushed[scope as usize][input as usize],
                            GraphOutput(output)      => &mut self.pointstamps.output_pushed[output as usize],
                        };
                        for summary in antichain.elements.iter() {
                            // println!("\ttarget: {:?} updated with {:?}", target, summary.results_in(&time));
                            dest.update(&summary.results_in(&time), value);
                        }
                    }
                }
            }

            for output in (0..self.pointstamps.source_counts[index].len()) {
                while let Some((time, value)) = self.pointstamps.source_counts[index][output as usize].pop() {
                    // println!("propagating: {}[{}]: {}", self.children[index].scope.name(), output, value);
                    for &(target, ref antichain) in self.source_summaries[index][output as usize].iter() {
                        let mut dest = match target {
                            ScopeInput(scope, input) => &mut self.pointstamps.target_pushed[scope as usize][input as usize],
                            GraphOutput(output)      => &mut self.pointstamps.output_pushed[output as usize],
                        };
                        for summary in antichain.elements.iter() {
                            // println!("\ttarget: {:?} updated with {:?}", target, summary.results_in(&time));
                            dest.update(&summary.results_in(&time), value);
                        }
                    }
                }
            }
        }

        for input in (0..self.inputs as usize) {                                        // for each graph inputs ...
            while let Some((time, value)) = self.pointstamps.input_counts[input].pop() {// for each update at GraphInput(input)...
                for &(target, ref antichain) in self.input_summaries[input].iter() {    // for each target it can reach ...
                    let mut dest = match target {
                        ScopeInput(scope, input) => &mut self.pointstamps.target_pushed[scope as usize][input as usize],
                        GraphOutput(output)      => &mut self.pointstamps.output_pushed[output as usize],
                    };
                    for summary in antichain.elements.iter() { dest.update(&summary.results_in(&time), value); }
                }
            }
        }
    }

    // Repeatedly takes edges (source, target), finds (target, source') connections,
    // expands based on (source', target') summaries.
    // Only considers targets satisfying the supplied predicate.
    fn set_summaries(&mut self) -> () {
        for scope in (0..self.children.len()) {
            for output in (0..self.children[scope].outputs as usize) {
                self.source_summaries[scope][output].clear();
                for &target in self.children[scope].edges[output].iter() {
                    if match target { ScopeInput(t, _) => self.children[t as usize].notify, _ => true } {
                        self.source_summaries[scope][output].push((target, Antichain::from_elem(self.default_summary)));
                    }
                }
            }
        }

        // load up edges from graph inputs
        for input in (0..self.inputs) {
            self.input_summaries[input as usize].clear();
            for &target in self.input_edges[input as usize].iter() {
                if match target { ScopeInput(t, _) => self.children[t as usize].notify, _ => true } {
                    self.input_summaries[input as usize].push((target, Antichain::from_elem(self.default_summary)));
                }
            }
        }

        let mut done = false;
        while !done {
            done = true;

            // process edges from scope outputs ...
            for scope in (0..self.children.len()) {                                         // for each scope
                for output in (0..self.children[scope].outputs) {                           // for each output
                    for target in self.children[scope].edges[output as usize].iter() {      // for each edge target
                        let next_sources = self.target_to_sources(target);
                        for &(next_source, next_summary) in next_sources.iter() {           // for each source it reaches
                            if let ScopeOutput(next_scope, next_output) = next_source {
                                // clone this so that we aren't holding a read ref to self.source_summaries.
                                let reachable = self.source_summaries[next_scope as usize][next_output as usize].clone();
                                for &(next_target, ref antichain) in reachable.iter() {
                                    for summary in antichain.elements.iter() {
                                        let cand_summary = next_summary.followed_by(summary);
                                        if try_to_add_summary(&mut self.source_summaries[scope][output as usize],next_target,cand_summary) {
                                            done = false;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // process edges from graph inputs ...
            for input in (0..self.inputs) {
                for target in self.input_edges[input as usize].iter() {
                    let next_sources = self.target_to_sources(target);
                    for &(next_source, next_summary) in next_sources.iter() {
                        if let ScopeOutput(next_scope, next_output) = next_source {
                            let reachable = self.source_summaries[next_scope as usize][next_output as usize].clone();
                            for &(next_target, ref antichain) in reachable.iter() {
                                for summary in antichain.elements.iter() {
                                    let candidate_summary = next_summary.followed_by(summary);
                                    if try_to_add_summary(&mut self.input_summaries[input as usize], next_target, candidate_summary) {
                                        done = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // now that we are done, populate self.target_summaries
        for scope in (0..self.children.len()) {
            for input in (0..self.children[scope].inputs) {
                self.target_summaries[scope][input as usize].clear();
                // first: add a link directly to the associate scope input (recently fixed)
                try_to_add_summary(&mut self.target_summaries[scope][input as usize], ScopeInput(scope as u64, input), Default::default());
                let next_sources = self.target_to_sources(&ScopeInput(scope as u64, input));
                for &(next_source, next_summary) in next_sources.iter() {
                    if let ScopeOutput(next_scope, next_output) = next_source {
                        for &(next_target, ref antichain) in self.source_summaries[next_scope as usize][next_output as usize].iter() {
                            for summary in antichain.elements.iter() {
                                let candidate_summary = next_summary.followed_by(summary);
                                try_to_add_summary(&mut self.target_summaries[scope][input as usize], next_target, candidate_summary);
                            }
                        }
                    }
                }
            }
        }
    }

    fn target_to_sources(&self, target: &Target) -> Vec<(Source, Summary<TOuter::Summary, TInner::Summary>)> {
        let mut result = Vec::new();

        match *target {
            GraphOutput(port) => {
                for input in (0..self.inputs()) {
                    for &summary in self.external_summaries[port as usize][input as usize].elements.iter() {
                        result.push((GraphInput(input), Outer(summary, Default::default())));
                    }
                }
            },
            ScopeInput(graph, port) => {
                for i in (0..self.children[graph as usize].outputs) {
                    for &summary in self.children[graph as usize].summary[port as usize][i as usize].elements.iter() {
                        result.push((ScopeOutput(graph, i), summary));
                    }
                }
            }
        }

        result
    }

    pub fn new_input(&mut self, shared_counts: Rc<RefCell<CountMap<Product<TOuter, TInner>>>>) -> u64 {
        self.inputs += 1;
        self.external_guarantee.push(MutableAntichain::new());
        self.input_messages.push(shared_counts);
        return self.inputs - 1;
    }

    pub fn new_output(&mut self) -> u64 {
        self.outputs += 1;
        self.external_capability.push(MutableAntichain::new());
        return self.outputs - 1;
    }

    pub fn connect(&mut self, source: Source, target: Target) {
        match source {
            ScopeOutput(scope, index) => { self.children[scope as usize].add_edge(index, target); },
            GraphInput(input) => {
                while (self.input_edges.len() as u64) < (input + 1)        { self.input_edges.push(Vec::new()); }
                self.input_edges[input as usize].push(target);
            },
        }
    }

    pub fn new_from<C: Communicator>(communicator: &mut C, index: u64, name: String) -> Subgraph<TOuter, TInner> {
        let progcaster = Progcaster::new(communicator);
        Subgraph {
            name:                   name,
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
            pointstamp_messages_cm: Default::default(),
            pointstamp_internal_cm: Default::default(),
            pointstamp_messages:    Default::default(),
            pointstamp_internal:    Default::default(),
            progcaster:             progcaster,
        }
    }

    fn print_reachability_summaries(&self)
    {
        println!("Reachability summary for subscope {}", self.name());
        println!("Scope outputs -> targets:");

        for i in 0..self.source_summaries.len() {
            for j in 0..self.source_summaries[i].len() {
                println!("\t{}.{} {}(output[{}]) ->", i, j, self.children[i].name(), j);
                for &(ref target, ref chain) in self.source_summaries[i][j].iter() {
                    println!("\t\t{}:\t{:?}", match target {
                        &ScopeInput(scope, input) => format!("{}[{}](input[{}])", self.children[scope as usize].name(), scope, input),
                        x => format!("{:?} ", x),
                    }, chain.elements);
                }
            }
        }

        println!("Scope inputs -> targets:");
        for i in 0..self.target_summaries.len() {
            for j in 0..self.target_summaries[i].len() {
                println!("\t{}.{} {}(input[{}]) ->", i, j, self.children[i].name(), j);
                for &(ref target, ref chain) in self.target_summaries[i][j].iter() {
                    println!("\t\t{}:\t{:?}", match target {
                        &ScopeInput(scope, input) => format!("{}[{}](input[{}])", self.children[scope as usize].name(), scope, input),
                        x => format!("{:?} ", x),
                        }, chain.elements);
                }
            }
        }
    }

    fn print_status(&self) {
        for child in self.children.iter() {
            for (index, messages) in child.outstanding_messages.iter().enumerate() {
                if messages.elements.len() > 0 {
                    println!("{}::{}.messages[{}]: {:?}", self.name(), child.name(), index, messages.elements);
                }
            }

            for (index, internal) in child.capabilities.iter().enumerate() {
                if internal.elements.len() > 0 {
                    println!("{}::{}.internal[{}]: {:?}", self.name(), child.name(), index, internal.elements);
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
