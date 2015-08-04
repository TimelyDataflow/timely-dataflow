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
use progress::nested::scope_wrapper::ScopeWrapper;
use progress::nested::pointstamp_counter::PointstampCounter;
use progress::nested::product::Product;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug)]
pub enum Source {
    GraphInput(usize),          // from outer scope
    ChildOutput(usize, usize),  // (scope, port) may have interesting connectivity
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug)]
pub enum Target {
    GraphOutput(usize),       // to outer scope
    ChildInput(usize, usize),   // (scope, port) may have interesting connectivity
}

pub struct Subgraph<TOuter:Timestamp, TInner:Timestamp> {
    pub name:               String,                     // a helpful name
    pub path:               String,
    pub index:              usize,                        // a useful integer

    default_summary:        Summary<TOuter::Summary, TInner::Summary>,    // default summary to use for something TODO: figure out what.

    inputs:                 usize,                        // number inputs into the scope
    outputs:                usize,                        // number outputs from the scope

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

    pointstamp_messages:    CountMap<(usize, usize, Product<TOuter, TInner>)>,
    pointstamp_internal:    CountMap<(usize, usize, Product<TOuter, TInner>)>,

    progcaster:             Progcaster<Product<TOuter, TInner>>,
}


impl<TOuter: Timestamp, TInner: Timestamp> Operate<TOuter> for Subgraph<TOuter, TInner> {
    fn name(&self) -> &str { &self.name }
    fn inputs(&self)  -> usize { self.inputs }
    fn outputs(&self) -> usize { self.outputs }

    // produces connectivity summaries from inputs to outputs, and reports initial internal
    // capabilities on each of the outputs (projecting capabilities from contained scopes).
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Vec<CountMap<TOuter>>) {

        // seal subscopes; prepare per-scope state/buffers
        for index in (0..self.children.len()) {
            let inputs  = self.children[index].inputs;
            let outputs = self.children[index].outputs;

            // initialize storage for vector-based source and target path summaries.
            self.source_summaries.push(vec![Vec::new(); outputs]);
            self.target_summaries.push(vec![Vec::new(); inputs]);

            self.pointstamps.target_pushed.push(vec![Default::default(); inputs]);
            self.pointstamps.target_counts.push(vec![Default::default(); inputs]);
            self.pointstamps.source_counts.push(vec![Default::default(); outputs]);

            // introduce capabilities as pre-pushed pointstamps; will push to outputs.
            for output in (0..outputs) {
                for time in self.children[index].capabilities[output].elements().iter(){
                    self.pointstamp_internal.update(&(index, output, time.clone()), 1);
                }
            }
        }

        // initialize space for input -> Vec<(Target, Antichain) mapping.
        self.input_summaries = vec![Vec::new(); self.inputs()];

        self.pointstamps.input_counts = vec![Default::default(); self.inputs()];
        self.pointstamps.output_pushed = vec![Default::default(); self.outputs()];

        self.external_summaries = vec![vec![Default::default(); self.inputs()]; self.outputs()];

        // TODO: Explain better.
        self.set_summaries();

        // self.push_pointstamps_to_outputs();
        self.push_pointstamps_to_scopes();

        self.pointstamp_internal.clear();

        // TODO: WTF is this all about? Who wrote this? Me...
        let mut work = vec![CountMap::new(); self.outputs()];
        for (output, map) in work.iter_mut().enumerate() {
            for &(ref key, val) in self.pointstamps.output_pushed[output].elements().iter() {
                map.update(&key.outer, val);
                self.external_capability[output].update(&key.outer, val);
            }
        }

        let mut summaries = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];

        for input in (0..self.inputs()) {
            for &(target, ref antichain) in self.input_summaries[input].iter() {
                if let GraphOutput(output) = target {
                    for &summary in antichain.elements.iter() {
                        summaries[input][output].insert(match summary {
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

    // receives connectivity summaries from outputs to inputs, as well as initial external
    // capabilities on inputs. prepares internal summaries, and recursively calls the method on
    // contained scopes.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<TOuter::Summary>>>, frontier: &mut [CountMap<TOuter>]) -> () {
        self.external_summaries = summaries;
        self.set_summaries();

        // change frontier to local times; introduce as pointstamps
        for graph_input in (0..self.inputs) {
            while let Some((time, val)) = frontier[graph_input].pop() {
                self.pointstamps.update_source(GraphInput(graph_input), &Product::new(time, Default::default()), val);
            }
        }

        // identify all capabilities expressed locally
        for scope in (0..self.children.len()) {
            for output in (0..self.children[scope].outputs) {
                for time in self.children[scope].capabilities[output].elements().iter() {
                    self.pointstamps.update_source(ChildOutput(scope, output), time, 1);
                }
            }
        }

        self.push_pointstamps_to_scopes();

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

            let inputs = self.children[subscope].inputs;
            let outputs = self.children[subscope].outputs;

            let mut summaries = vec![vec![Antichain::new(); inputs]; outputs];

            for output in (0..summaries.len()) {
                for &(target, ref antichain) in self.source_summaries[subscope][output].iter() {
                    if let ChildInput(target_scope, target_input) = target {
                        if target_scope == subscope { summaries[output][target_input] = antichain.clone() }
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
                self.pointstamps.update_source(GraphInput(input), &Product::new(time, Default::default()), val);
            }
        }

        self.push_pointstamps_to_scopes();

        // consider pushing to each nested scope in turn.
        for (index, child) in self.children.iter_mut().enumerate() {
            child.push_pointstamps(&self.pointstamps.target_pushed[index]);
        }

        self.pointstamps.clear_pushed();
    }

    // information from the vertex about its progress (updates to the output frontiers, recv'd and sent message counts)
    // the results of this method are meant to reflect state *internal* to the local instance of this
    // scope; although the scope may exchange progress information to determine whether something is runnable,
    // the exchanged information should probably *not* be what is reported back. only the locally
    // produced/consumed messages, and local internal work should be reported back up.
    fn pull_internal_progress(&mut self, internal_progress: &mut [CountMap<TOuter>],
                                         messages_consumed: &mut [CountMap<TOuter>],
                                         messages_produced: &mut [CountMap<TOuter>]) -> bool {
        // should be false when there is nothing left to do
        let mut active = false;

        // Step 1: handle messages introduced through each graph input
        for input in (0..self.inputs) {
            let mut borrowed = self.input_messages[input].borrow_mut();
            while let Some((time, delta)) = borrowed.pop() {
                messages_consumed[input].update(&time.outer, delta);
                for &target in self.input_edges[input].iter() {
                    match target {
                        ChildInput(tgt, tgt_in)   => { self.pointstamp_messages.update(&(tgt, tgt_in, time), delta); },
                        GraphOutput(graph_output) => { messages_produced[graph_output].update(&time.outer, delta); },
                    }
                }
            }
        }

        // Step 2: pull_internal_progress from subscopes.
        for child in self.children.iter_mut() {
            let subactive = child.pull_pointstamps(&mut self.pointstamp_messages,
                                                   &mut self.pointstamp_internal,
                                                   |out, time, delta| { messages_produced[out].update(&time.outer, delta); });

            active = active || subactive;
        }

        // if self.pointstamp_messages.len() > 0 || self.pointstamp_internal.len() > 0 {
        //     println!("{}: messages: {:?}", self.path, self.pointstamp_messages);
        //     println!("{}: internal: {:?}", self.path, self.pointstamp_internal);
        // }


        // Intermission: exchange pointstamp updates, and then move them to the pointstamps structure.
        self.progcaster.send_and_recv(&mut self.pointstamp_messages, &mut self.pointstamp_internal);

        {
            let pointstamps = &mut self.pointstamps;
            while let Some(((scope, input, time), delta)) = self.pointstamp_messages.pop() {
                self.children[scope].outstanding_messages[input].update_and(&time, delta, |time, delta| {
                    pointstamps.update_target(ChildInput(scope, input), time, delta);
                });
            }
            while let Some(((scope, output, time), delta)) = self.pointstamp_internal.pop() {
                self.children[scope].capabilities[output].update_and(&time, delta, |time, delta| {
                    pointstamps.update_source(ChildOutput(scope, output), time, delta);
                });
            }
        }

        // Step 3: push progress to each graph output (important: be very clear about where this happens)
        // self.push_pointstamps_to_outputs();
        self.push_pointstamps_to_scopes();

        for output in (0..self.outputs) {
            // let name = self.path.clone();
            while let Some((time, val)) = self.pointstamps.output_pushed[output as usize].pop() {
                // println!("{}: output: {}, time: {:?}, update: {}", name, output, time, val);
                self.external_capability[output as usize].update_and(&time.outer, val, |t,v| {
                    // println!("{}  done: {}, time: {:?}, update: {}", name, output, t, v);
                    internal_progress[output as usize].update(t, v);
                });
            }
        }


        // Step 4: push any progress to each target subgraph ...
        for (index, child) in self.children.iter_mut().enumerate() {
            child.push_pointstamps(&self.pointstamps.target_pushed[index][..]);
        }

        self.pointstamps.clear_pushed();

        // if there are outstanding messages or capabilities, we must insist on continuing to run
        for child in self.children.iter() {
            active = active || child.outstanding_messages.iter().any(|x| x.elements().len() > 0);
            active = active || child.capabilities.iter().any(|x| x.elements().len() > 0);
        }

        // // if we want to see why we are active
        // if active { self._print_status(); }

        // if internal_progress.iter().any(|x| x.len() > 0)
        // || messages_consumed.iter().any(|x| x.len() > 0)
        // || messages_produced.iter().any(|x| x.len() > 0) {
        //     println!("{}: returning with\ninternal_progress: {:?}\nmessages_consumed: {:?}\nmessages_produced: {:?}",
        //          self.path, internal_progress, messages_consumed, messages_produced);
        // }

        return active;
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Subgraph<TOuter, TInner> {
    pub fn children(&self) -> usize { self.children.len() }

    // pushes pointstamps to scopes; clears pre-push counts
    // TODO : Differentiate connectivity by scope/output to avoid "if let"  in core loop
    fn push_pointstamps_to_scopes(&mut self) -> () {
        for index in (0..self.children.len()) {
            for input in (0..self.pointstamps.target_counts[index].len()) {
                while let Some((time, value)) = self.pointstamps.target_counts[index][input].pop() {
                    for &(target, ref antichain) in self.target_summaries[index][input].iter() {
                        if let ChildInput(scope, input) = target {
                            for summary in antichain.elements.iter() {
                                self.pointstamps.target_pushed[scope as usize][input as usize].update(&summary.results_in(&time), value);
                            }
                        }
                        if let GraphOutput(output) = target {
                            for summary in antichain.elements.iter() {
                                self.pointstamps.output_pushed[output as usize].update(&summary.results_in(&time), value);
                            }
                        }
                    }
                }
            }

            for output in (0..self.pointstamps.source_counts[index].len()) {
                while let Some((time, value)) = self.pointstamps.source_counts[index][output].pop() {
                    for &(target, ref antichain) in self.source_summaries[index][output].iter() {
                        if let ChildInput(scope, input) = target {
                            for summary in antichain.elements.iter() {
                                self.pointstamps.target_pushed[scope as usize][input as usize].update(&summary.results_in(&time), value);
                            }
                        }
                        if let GraphOutput(output) = target {
                            for summary in antichain.elements.iter() {
                                self.pointstamps.output_pushed[output as usize].update(&summary.results_in(&time), value);
                            }
                        }
                    }
                }
            }
        }

        for input in (0..self.inputs as usize) {
            while let Some((time, value)) = self.pointstamps.input_counts[input].pop() {
                for &(target, ref antichain) in self.input_summaries[input].iter() {
                    if let ChildInput(scope, input) = target {
                        for summary in antichain.elements.iter() {
                            self.pointstamps.target_pushed[scope as usize][input as usize].update(&summary.results_in(&time), value);
                        }
                    }
                    if let GraphOutput(output) = target {
                        for summary in antichain.elements.iter() {
                            self.pointstamps.output_pushed[output as usize].update(&summary.results_in(&time), value);
                        }
                    }
                }
            }
        }
    }

    // // pushes pointstamps to graph outputs; does not clear any of the pre-push counts
    // // TODO : Differentiate connectivity by scope/output to avoid "if let"  in core loop
    // fn push_pointstamps_to_outputs(&mut self) -> () {
    //     for &((index, input, ref time), value) in self.pointstamp_messages.elements().iter() {
    //         for &(target, ref antichain) in self.target_summaries[index as usize][input as usize].iter() {
    //             if let GraphOutput(output) = target {
    //                 for summary in antichain.elements.iter() {
    //                     self.pointstamps.output_pushed[output as usize].update(&summary.results_in(&time), value);
    //                 }
    //             }
    //         }
    //     }
    //
    //     for &((index, output, ref time), value) in self.pointstamp_internal.elements().iter() {
    //         for &(target, ref antichain) in self.source_summaries[index as usize][output as usize].iter() {
    //             if let GraphOutput(output) = target {
    //                 for summary in antichain.elements.iter() {
    //                     self.pointstamps.output_pushed[output as usize].update(&summary.results_in(&time), value);
    //                 }
    //             }
    //         }
    //     }
    // }

    // Repeatedly takes edges (source, target), finds (target, source') connections,
    // expands based on (source', target') summaries.
    // Only considers targets satisfying the supplied predicate.
    fn set_summaries(&mut self) -> () {

        for scope in (0..self.children.len()) {
            for output in (0..self.children[scope].outputs) {
                self.source_summaries[scope][output].clear();
                for &target in self.children[scope].edges[output].iter() {
                    if match target { ChildInput(t, _) => self.children[t].notify, _ => true } {
                        self.source_summaries[scope][output].push((target, Antichain::from_elem(self.default_summary)));
                    }
                }
            }
        }

        // load up edges from graph inputs
        for input in (0..self.inputs) {
            // if input >= self.input_edges.len() { println!("oops 1"); }
            // if input >= self.input_summaries.len() { println!("oops 2"); }
            self.input_summaries[input].clear();
            for &target in self.input_edges[input].iter() {
                if match target { ChildInput(t, _) => self.children[t].notify, _ => true } {
                    self.input_summaries[input].push((target, Antichain::from_elem(self.default_summary)));
                }
            }
        }

        let mut done = false;
        while !done {
            done = true;

            // process edges from scope outputs ...
            for scope in (0..self.children.len()) {                                         // for each scope
                for output in (0..self.children[scope].outputs) {                           // for each output
                    for target in self.children[scope].edges[output].iter() {      // for each edge target
                        let next_sources = self.target_to_sources(target);
                        for &(next_source, next_summary) in next_sources.iter() {           // for each source it reaches
                            if let ChildOutput(next_scope, next_output) = next_source {
                                // clone this so that we aren't holding a read ref to self.source_summaries.
                                let reachable = self.source_summaries[next_scope][next_output].clone();
                                for &(next_target, ref antichain) in reachable.iter() {
                                    for summary in antichain.elements.iter() {
                                        let cand_summary = next_summary.followed_by(summary);
                                        if try_to_add_summary(&mut self.source_summaries[scope][output],next_target,cand_summary) {
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
                for target in self.input_edges[input].iter() {
                    let next_sources = self.target_to_sources(target);
                    for &(next_source, next_summary) in next_sources.iter() {
                        if let ChildOutput(next_scope, next_output) = next_source {
                            let reachable = self.source_summaries[next_scope][next_output].clone();
                            for &(next_target, ref antichain) in reachable.iter() {
                                for summary in antichain.elements.iter() {
                                    let candidate_summary = next_summary.followed_by(summary);
                                    if try_to_add_summary(&mut self.input_summaries[input], next_target, candidate_summary) {
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
                self.target_summaries[scope][input].clear();
                // first: add a link directly to the associate scope input
                try_to_add_summary(&mut self.target_summaries[scope][input], ChildInput(scope, input), Default::default());
                let next_sources = self.target_to_sources(&ChildInput(scope, input));
                for &(next_source, next_summary) in next_sources.iter() {
                    if let ChildOutput(next_scope, next_output) = next_source {
                        for &(next_target, ref antichain) in self.source_summaries[next_scope][next_output].iter() {
                            for summary in antichain.elements.iter() {
                                let candidate_summary = next_summary.followed_by(summary);
                                try_to_add_summary(&mut self.target_summaries[scope][input], next_target, candidate_summary);
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
            ChildInput(graph, port) => {
                for i in (0..self.children[graph as usize].outputs) {
                    for &summary in self.children[graph as usize].summary[port as usize][i as usize].elements.iter() {
                        result.push((ChildOutput(graph, i), summary));
                    }
                }
            }
        }

        result
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

    pub fn new_from<A: Allocate>(allocator: &mut A, index: usize, path: String) -> Subgraph<TOuter, TInner> {
        let progcaster = Progcaster::new(allocator);
        Subgraph {
            name:                   format!("Subgraph"),
            path:                   format!("{}::Subgraph[{}]", path, index),
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

    fn _print_reachability_summaries(&self) {
        println!("Reachability summary for subscope {}", self.name());
        println!("Operate outputs -> targets:");

        for i in 0..self.source_summaries.len() {
            for j in 0..self.source_summaries[i].len() {
                println!("\t{}.{} {}(output[{}]) ->", i, j, self.children[i].name(), j);
                for &(ref target, ref chain) in self.source_summaries[i][j].iter() {
                    println!("\t\t{}:\t{:?}", match target {
                        &ChildInput(scope, input) => format!("{}[{}](input[{}])", self.children[scope as usize].name(), scope, input),
                        x => format!("{:?} ", x),
                    }, chain.elements);
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
                        }, chain.elements);
                }
            }
        }
    }

    fn _print_status(&self) {
        println!("printing status for {}", self.path);
        for child in self.children.iter() {
            for (index, messages) in child.outstanding_messages.iter().enumerate() {
                if messages.elements().len() > 0 {
                    println!("  ({})::{}.messages[{}]: {:?}", self.path, child.name(), index, messages.elements());
                }
            }

            for (index, internal) in child.capabilities.iter().enumerate() {
                if internal.elements().len() > 0 {
                    println!("  ({})::{}.internal[{}]: {:?}", self.path, child.name(), index, internal.elements());
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
