//! Implements `Operate` for a scoped collection of child operators.

use std::default::Default;
use std::fmt::Debug;

use std::rc::Rc;
use std::cell::RefCell;
use timely_communication::Allocate;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, PathSummary, Operate};

use progress::count_map::CountMap;

use progress::broadcast::Progcaster;
use progress::nested::summary::Summary;
use progress::nested::summary::Summary::{Local, Outer};
// use progress::nested::scope_wrapper::ChildWrapper;
use progress::nested::pointstamp_counter::PointstampCounter;
use progress::nested::product::Product;

// IMPORTANT : by convention, a child identifier of zero is used to indicate inputs and outputs of
// the Subgraph itself. An identifier greater than zero corresponds to an actual child, which can
// be found at position (id - 1) in the `children` field of the Subgraph.

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Source { pub index: usize, pub port: usize, }
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Target { pub index: usize, pub port: usize, }

pub struct Subgraph<TOuter:Timestamp, TInner:Timestamp> {

    name: String,
    pub path: Vec<usize>,
    pub index: usize,

    inputs: usize,
    outputs: usize,

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<Product<TOuter, TInner>>>,
    child_count: usize,

    edge_stash: Vec<(Source, Target)>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<CountMap<Product<TOuter, TInner>>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    // pointstamp messages to exchange. ultimately destined for `messages` or `internal`.
    local_pointstamp_messages: CountMap<(usize, usize, Product<TOuter, TInner>)>,
    local_pointstamp_internal: CountMap<(usize, usize, Product<TOuter, TInner>)>,
    final_pointstamp_messages: CountMap<(usize, usize, Product<TOuter, TInner>)>,
    final_pointstamp_internal: CountMap<(usize, usize, Product<TOuter, TInner>)>,

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
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Vec<CountMap<TOuter>>) {

        // println!("in GIS for subgraph: {:?}", self.path);

        // at this point, the subgraph is frozen. we should initialize any internal state which
        // may have been determined after construction (e.g. the numbers of inputs and outputs).
        // we also need to determine what to return as a summary and initial capabilities, which
        // will depend on child summaries and capabilities, as well as edges in the subgraph.

        // perhaps first check that the children are saney identified
        self.children.sort_by(|x,y| x.index.cmp(&y.index));
        assert!(self.children.iter().enumerate().all(|(i,x)| i == x.index));

        while let Some((source, target)) = self.edge_stash.pop() {
            // println!("  edge: {:?}", (source, target));
            self.children[source.index].edges[source.port].push(target);
        }

        // set up a bit of information about the "0th" child, reflecting the subgraph's external
        // connections
        assert_eq!(self.children[0].outputs, self.inputs());
        assert_eq!(self.children[0].inputs, self.outputs());

        // seal subscopes; prepare per-scope state/buffers
        for child in &self.children {
            self.pointstamps.allocate_for_operator(child.inputs, child.outputs);

            // introduce capabilities as pre-pushed pointstamps; will push to outputs.
            for (output, capability) in child.internal.iter().enumerate() {
                for time in capability.elements() {
                    // println!("  child {} output {} has capability: {:?}", child.index, output, time);
                    self.pointstamps.source[child.index][output].update(time, 1);
                    // self.final_pointstamp_internal.update(&(child.index, output, time.clone()), 1);
                }
            }
        }

        self.compute_summaries();   // determine summaries in the absence of external connectivity.
        self.push_pointstamps();    // push capabilities forward to determine output capabilities.

        // the initial capabilities should now be in `self.pointstamps.target_pushed[0]`, except
        // that they are `Product<TOuter, TInner>`, and we just want `TOuter`.
        let mut initial_capabilities = vec![CountMap::new(); self.outputs()];
        for (o_port, capabilities) in self.pointstamps.pushed[0].iter().enumerate() {
            for &(time, val) in capabilities.elements() {
                // make a note to self and inform scope of our capability.
                self.output_capabilities[o_port].update(&time.outer, val);
                initial_capabilities[o_port].update(&time.outer, val);
            }
        }
        // done with the pointstamps, so we should clean up.
        self.pointstamps.clear();

        // summarize the scope internals by looking for source_target_summaries from id 0 to id 0.
        let mut internal_summary = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];
        for input in 0..self.inputs() {
            for &(target, ref antichain) in self.children[0].source_target_summaries[input].iter() {
                if target.index == 0 {
                    for &summary in antichain.elements().iter() {
                        internal_summary[input][target.port].insert(match summary {
                            Local(_)    => Default::default(),
                            Outer(y, _) => y,
                        });
                    };
                }
            }
        }

        return (internal_summary, initial_capabilities);
    }

    // receives connectivity summaries from outputs to inputs, as well as initial external
    // capabilities on inputs. prepares internal summaries, and recursively calls the method on
    // contained scopes.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<TOuter::Summary>>>, frontier: &mut [CountMap<TOuter>]) {

        // println!("in SES for subgraph: {:?}", self.path);
        // for (input, frontier) in frontier.iter().enumerate() {
        //     println!(" input {} frontier: {:?}", input, frontier);
        // }

        // we must now finish the work of setting up the subgraph, using the external summary and
        // external capabilities on the subgraph's inputs.
        for output in 0..self.outputs {
            for input in 0..self.inputs {
                for &summary in summaries[output][input].elements() {
                    try_to_add_summary(
                        &mut self.children[0].target_source_summaries[output],
                        Source { index: 0, port: input },
                        Outer(summary, Default::default())
                    );
                }
            }
        }

        // now re-compute all summaries with `self.children[0].target_source_summaries` set.
        self.compute_summaries();

        // filter out the external -> external links, as we only want to summarize contained scopes
        for summaries in &mut self.children[0].target_target_summaries {
            summaries.retain(|&(t, _)| t.index > 0);
        }
        for summaries in &mut self.children[0].source_target_summaries {
            summaries.retain(|&(t, _)| t.index > 0);
        }

        // initialize pointstamps for capabilities, from `frontier` and from child capabitilies.
        // change frontier to local times; introduce as pointstamps
        for input in 0..self.inputs {
            while let Some((time, val)) = frontier[input].pop() {
                self.children[0].internal[input].update_and(&Product::new(time, Default::default()), val, |_t, _v| {
                    // println!("initializing external input[{}] at {:?} with {}", input, _t, _v);
                });
            }
        }

        // identify all capabilities expressed locally
        for child in &self.children {
            for output in 0..child.outputs {
                for time in child.internal[output].elements().iter() {
                    // println!("internal capability expressed! ({}, {}): {:?}", child.index, output, time);
                    self.pointstamps.update_source(
                        Source { index: child.index, port: output },
                        time,
                        1
                    );
                }
            }
        }

        self.push_pointstamps();

        // we now have summaries and pushed pointstamps, which should be sufficient to recursively
        // call `set_external_summary` for each of the children.

        for child in &mut self.children {

            // set the initial capabilities based on the contents of self.pointstamps.target_pushed[child.index]
            for input in 0..child.inputs {
                while let Some((time, val)) = self.pointstamps.pushed[child.index][input].pop() {
                    // println!("found a pushed pointstamp! ({}, {}): {:?}", child.index, input, time);
                    // pushes changes through `external[input]` into `external_buffer[input]`.
                    // at this point all changes should be meaningful, but need to prime state.
                    let buffer = &mut child.external_buffer[input];
                    child.external[input].update_and(&time, val, |t, v| { buffer.update(t, v); });
                }
            }

            // summarize the subgraph by the path summaries from the child's output to its inputs.
            let mut summary = vec![vec![Antichain::new(); child.inputs]; child.outputs];
            for output in 0..child.outputs {
                for &(source, ref antichain) in &child.source_target_summaries[output] {
                    if source.index == child.index {
                        summary[output][source.port] = antichain.clone();
                    }
                }
            }

            child.set_external_summary(summary);
        }

        // clean up after ourselves.
        self.pointstamps.clear();
    }

    // changes in the message possibilities for each of the subgraph's inputs.
    fn push_external_progress(&mut self, external: &mut [CountMap<TOuter>]) {

        // I believe we can simply move these into our pointstamp staging area.
        // Nothing will happen until we call `step`, but that is to be expected.
        for (port, changes) in external.iter_mut().enumerate() {
            while let Some((time, val)) = changes.pop() {
                // println!("updating external input[{}] at {:?} with {}", port, time, val);
                let pointstamps = &mut self.pointstamps;
                self.children[0].internal[port].update_and(&Product::new(time, Default::default()), val, |t, v| {
                    // println!("  difference made![{}] at {:?} with {}", port, t, v);
                    pointstamps.update_source(Source { index: 0, port: port }, t, v)
                });
            }
        }

        // we should also take this opportunity to clean out any messages in self.children[0].messages.
        // it isn't exactly correct that we are sure that they have been acknowledged, but ... we don't
        // have enough information in the api at the moment to be certain. we could add it, but for now
        // we assume that a call to `push_external_progress` reflects all updates previously produced by
        // `pull_internal_progress`.
        for (port, messages) in self.children[0].messages.iter_mut().enumerate() {
            for time in messages.elements() {
                self.pointstamps.update_target(Target { index: 0, port: port }, time, -1)
            }
            messages.clear();
        }
    }

    // information from the vertex about its progress (updates to the output frontiers, recv'd and sent message counts)
    // the results of this method are meant to reflect state *internal* to the local instance of this
    // scope; although the scope may exchange progress information to determine whether something is runnable,
    // the exchanged information should probably *not* be what is reported back. only the locally
    // produced/consumed messages, and local internal work should be reported back up.
    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<TOuter>],
                                         internal: &mut [CountMap<TOuter>],
                                         produced: &mut [CountMap<TOuter>]) -> bool
    {
        // should be false when there is nothing left to do
        let mut active = false;

        // Step 1: Observe the number of records this instances has accepted as input.
        //
        // This information should be exchanged with peers, probably as subtractions to the output
        // "capabilities" of the input ports. This is a bit non-standard, but ... it makes sense.
        // The "resulting" increments to message counts should also be exchanged at the same time.
        for input in 0..self.inputs {
            let mut borrowed = self.input_messages[input].borrow_mut();
            while let Some((time, delta)) = borrowed.pop() {
                self.local_pointstamp_internal.update(&(0, input, time), delta);
                for target in self.children[0].edges[input].iter() {
                    self.local_pointstamp_messages.update(&(target.index, target.port, time), delta);
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
                    &mut self.final_pointstamp_messages,
                    &mut self.final_pointstamp_internal,
                )
            };

            // if subactive { println!("child {} demands activity!", child.name); }

            active = active || subactive;
        }

        // Intermission: exchange pointstamp updates, and then move them to the pointstamps structure.
        //
        // Note : Not all pointstamps should be exchanged. Some should be immediately installed if
        // they correspond to scopes that are not .local(). These scopes represent operators that
        // have already exchanged their progress information, and whose messages already reflect
        // full information.

        // for x in self.local_pointstamp_messages.elements() {
        //     println!("{:?}\tlocal message: {:?}", self.path, x);
        // }
        // for x in self.local_pointstamp_internal.elements() {
        //     println!("{:?}\tlocal internal: {:?}", self.path, x);
        // }

        // exchange progress messages that need to be exchanged.
        self.progcaster.send_and_recv(
            &mut self.local_pointstamp_messages,
            &mut self.local_pointstamp_internal,
        );

        // at this point we may need to do something horrible and filthy. Unfortunately, I think
        // this very filthy thing is what we need to overcome the prior bug in progress tracking
        // logic: we need to bundle the communication about messages consumed with statements about
        // where the messages show up, rather than have a parent scope manage the messages consumed
        // information, which leads one "progress transaction" split across two paths.

        // we have exchanged "capabilities" from the subgraph inputs, which we used to indicate
        // messages received by this instance. They are not capabilities, and should not be pushed
        // as pointstamps. Rather the contents of `self.external_external_buffer` should be pushed
        // as pointstamps. So, at this point it will be important to carefully remove elements of
        // `self.local_pointstamp_internal` with index 0, and introduce elements from `external_*`.
        // at least, the `drain_into` we are about to do should perform different logic as needed.

        // fold exchanged messages into the view of global progress.
        self.local_pointstamp_messages.drain_into(&mut self.final_pointstamp_messages);
        while let Some(((index, port, timestamp), delta)) = self.local_pointstamp_internal.pop() {
            if index == 0 { consumed[port].update(&timestamp.outer, delta); }
            else { self.final_pointstamp_internal.update(&(index, port, timestamp), delta); }
        }

        // for x in self.final_pointstamp_messages.elements() {
        //     println!("{:?}\tfinal message: {:?}", self.path, x);
        // }
        // for x in self.final_pointstamp_internal.elements() {
        //     println!("{:?}\tfinal internal: {:?}", self.path, x);
        // }

        // Having exchanged progress updates, push message and capability updates through a filter
        // which indicates changes to the discrete frontier of the the antichain. This suppresses
        // changes that do not change the discrete frontier, even if they change counts or add new
        // elements beyond the frontier.
        while let Some(((index, input, time), delta)) = self.final_pointstamp_messages.pop() {
            let pointstamps = &mut self.pointstamps;
            self.children[index].messages[input].update_and(&time, delta, |time, delta|
                pointstamps.update_target(Target { index: index, port: input }, time, delta)
            );
            // if the message went to the output, tell someone I guess.
            if index == 0 { produced[input].update(&time.outer, delta); }
        }
        while let Some(((index, output, time), delta)) = self.final_pointstamp_internal.pop() {
            let pointstamps = &mut self.pointstamps;
            // println!("attempting internal update for {}[{}][{}]: {:?} by {}", self.children[index].name, index, output, time, delta);
            // println!("  current state: {:?}", self.children[index].internal[output]);
            self.children[index].internal[output].update_and(&time, delta, |time, delta| {
                // println!("  difference made! {:?} {}", time, delta);
                pointstamps.update_source(Source { index: index, port: output }, time, delta);
            });
        }

        // push implications of outstanding work at each location to other locations in the graph.
        self.push_pointstamps();

        // update input capabilities for actual children.
        for child in self.children.iter_mut().skip(1) {
            let pointstamps = &self.pointstamps.pushed[child.index][..];
            child.push_pointstamps(&pointstamps);
        }

        // produce output data for `internal`. `consumed` and `produced` have already been filled.
        for (output, pointstamps) in self.pointstamps.pushed[0].iter_mut().enumerate() {
            while let Some((time, val)) = pointstamps.pop() {
                self.output_capabilities[output].update_and(&time.outer, val, |t,v|
                    { internal[output].update(t, v); }
                );
            }
        }

        self.pointstamps.clear();

        // if there are outstanding messages or capabilities, we must insist on continuing to run
        for child in self.children.iter() {
            // if child.messages.iter().any(|x| x.elements().len() > 0) 
            // { println!("{:?}: child {}[{}] has outstanding messages", self.path, child.name, child.index); }
            active = active || child.messages.iter().any(|x| x.elements().len() > 0);
            // if child.internal.iter().any(|x| x.elements().len() > 0) 
            // { println!("{:?} child {}[{}] has outstanding capabilities", self.path, child.name, child.index); }
            active = active || child.internal.iter().any(|x| x.elements().len() > 0);
        }

        return active;
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Subgraph<TOuter, TInner> {

    // pushes pointstamps to scopes using self.*_summaries; clears pre-push counts.
    fn push_pointstamps(&mut self) {

        for index in 0..self.pointstamps.target.len() {
            for input in 0..self.pointstamps.target[index].len() {
                while let Some((time, value)) = self.pointstamps.target[index][input].pop() {
                    for &(target, ref antichain) in self.children[index].target_target_summaries[input].iter() {
                        for summary in antichain.elements().iter() {
                            self.pointstamps.pushed[target.index][target.port].update(&summary.results_in(&time), value);
                        }
                    }
                }
            }
        }

        // push pointstamps from sources to targets.
        for index in 0..self.pointstamps.source.len() {
            for output in 0..self.pointstamps.source[index].len() {
                while let Some((time, value)) = self.pointstamps.source[index][output].pop() {
                    for &(target, ref antichain) in self.children[index].source_target_summaries[output].iter() {
                        for summary in antichain.elements().iter() {
                            self.pointstamps.pushed[target.index][target.port].update(&summary.results_in(&time), value);
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
        //
        // for child_index in 0..self.children.len() {
        //     println!("child[{}]: {}", child_index, self.children[child_index].name);
        //     for output in 0..self.children[child_index].outputs {
        //         println!("  output[{}] edges", output);
        //         for x in &self.children[child_index].edges[output] {
        //             println!("    {:?}", x);
        //         }
        //     }
        // }


        // make sure we have allocated enough space in self.pointstamps
        self.pointstamps.source = vec![Vec::new(); self.children.len()];
        self.pointstamps.target = vec![Vec::new(); self.children.len()];
        self.pointstamps.pushed = vec![Vec::new(); self.children.len()];

        for child in &self.children {
            self.pointstamps.source[child.index] = vec![Default::default(); child.outputs];
            self.pointstamps.target[child.index] = vec![Default::default(); child.inputs];
            self.pointstamps.pushed[child.index] = vec![Default::default(); child.inputs];
        }

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
                    let summary = summary.followed_by(new_summary);
                    let edges = self.children[new_source.index].edges[new_source.port].clone();
                    for &new_target in &edges {
                        if try_to_add_summary(&mut self.children[source.index].source_target_summaries[source.port], new_target, summary) {
                            additions.push_back((source, new_target, summary));
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
                                let summary = summary.followed_by(new_summary);
                                try_to_add_summary(&mut self.children[child_index].target_target_summaries[input], target, summary);
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

    pub fn new_input(&mut self, shared_counts: Rc<RefCell<CountMap<Product<TOuter, TInner>>>>) -> usize {
        self.inputs += 1;
        self.input_messages.push(shared_counts);
        self.children[0].add_output();
        return self.inputs - 1;
    }

    pub fn new_output(&mut self) -> usize {
        self.outputs += 1;
        self.output_capabilities.push(MutableAntichain::new());
        self.children[0].add_input();
        return self.outputs - 1;
    }

    pub fn connect(&mut self, source: Source, target: Target) {
        self.edge_stash.push((source, target));
    }



    pub fn new_from<A: Allocate>(allocator: &mut A, index: usize, mut path: Vec<usize>) -> Subgraph<TOuter, TInner> {
        let progcaster = Progcaster::new(allocator);
        path.push(index);

        let children = vec![PerOperatorState::empty(path.clone())];

        Subgraph {
            name:                   format!("Subgraph"),
            path:                   path,
            index:                  index,

            inputs:                 Default::default(),
            outputs:                Default::default(),

            children:               children,
            child_count:            1,
            edge_stash: vec![],

            input_messages:         Default::default(),
            output_capabilities:    Default::default(),

            pointstamps:            Default::default(),
            local_pointstamp_messages:    Default::default(),
            local_pointstamp_internal:    Default::default(),
            final_pointstamp_messages:    Default::default(),
            final_pointstamp_internal:    Default::default(),
            progcaster:             progcaster,
        }
    }

    pub fn allocate_child_id(&mut self) -> usize {
        self.child_count += 1;
        // println!("returning allocation: {}", self.child_count - 1);
        return self.child_count - 1;
    }

    pub fn add_child(&mut self, child: Box<Operate<Product<TOuter, TInner>>>, index: usize, identifier: usize) {
        self.children.push(PerOperatorState::new(child, index, self.path.clone(), identifier))
    }
}

fn try_to_add_summary<T: Eq, S: PartialOrd+Eq+Copy+Debug>(vector: &mut Vec<(T, Antichain<S>)>, target: T, summary: S) -> bool {
    for &mut (ref t, ref mut antichain) in vector.iter_mut() {
        if target.eq(t) { return antichain.insert(summary); }
    }
    vector.push((target, Antichain::from_elem(summary)));
    return true;
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

    consumed_buffer: Vec<CountMap<T>>, // per-input: temp buffer used for pull_internal_progress.
    internal_buffer: Vec<CountMap<T>>, // per-output: temp buffer used for pull_internal_progress.
    produced_buffer: Vec<CountMap<T>>, // per-output: temp buffer used for pull_internal_progress.

    external_buffer: Vec<CountMap<T>>, // per-input: temp buffer used for push_external_progress.
}

impl<T: Timestamp> PerOperatorState<T> {

    fn add_input(&mut self) {
        self.inputs += 1;
        self.target_target_summaries.push(vec![]);
        self.target_source_summaries.push(vec![]);
        self.messages.push(Default::default());
        self.external.push(Default::default());
        self.external_buffer.push(Default::default());
        self.consumed_buffer.push(Default::default());
    }
    fn add_output(&mut self) {
        self.outputs += 1;
        self.edges.push(vec![]);
        self.internal.push(Default::default());
        self.internal_buffer.push(Default::default());
        self.produced_buffer.push(Default::default());
        self.source_target_summaries.push(vec![]);
    }

    fn empty(mut path: Vec<usize>) -> PerOperatorState<T> {
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
        }
    }

    pub fn new(mut scope: Box<Operate<T>>, index: usize, mut path: Vec<usize>, identifier: usize) -> PerOperatorState<T> {

        // LOGGING
        path.push(index);

        ::logging::log(&::logging::OPERATES, ::logging::OperatesEvent { id: identifier, addr: path.clone(), name: scope.name().to_owned() });

        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let (summary, work) = scope.get_internal_summary();

        assert!(summary.len() == inputs);
        assert!(!summary.iter().any(|x| x.len() != outputs));

        let mut new_summary = vec![Vec::new(); inputs];
        for input in 0..inputs {
            for output in 0..outputs {
                for &summary in summary[input][output].elements() {
                    try_to_add_summary(&mut new_summary[input], Source { index: index, port: output }, summary);
                }
            }
        }

        let mut result = PerOperatorState {
            name:       scope.name(),
            // addr:       path,
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

            external_buffer: vec![Default::default(); inputs],

            consumed_buffer: vec![CountMap::new(); inputs],
            internal_buffer: vec![CountMap::new(); outputs],
            produced_buffer: vec![CountMap::new(); outputs],

            target_target_summaries: vec![vec![]; inputs],
            source_target_summaries: vec![vec![]; outputs],

            target_source_summaries:    new_summary,
        };

        // TODO : Gross. Fix.
        for (index, capability) in result.internal.iter_mut().enumerate() {
            capability.update_iter_and(work[index].elements().iter().map(|x|x.clone()), |_, _| {});
        }

        return result;
    }

    pub fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<T::Summary>>>) {
        let frontier = &mut self.external_buffer;
        if self.index > 0 && self.notify && frontier.len() > 0 && !frontier.iter().any(|x| x.len() > 0) {
            println!("initializing notifiable operator {}[{}] with inputs but no external capabilities", self.name, self.index);
        }
        self.operator.as_mut().map(|scope| scope.set_external_summary(summaries, frontier));
    }


    pub fn push_pointstamps(&mut self, external_progress: &[CountMap<T>]) {

        // assert the "uprighted-ness" property for updates to each input
        // doesn't need to be true here; only after titration through self.external
        // for (index, updates) in external_progress.iter().enumerate() {
        //     for &(time, val) in updates.elements() {
        //         if val > 0 {
        //             if !self.external[index].elements().iter().any(|&t2| t2.le(&time)) {
        //                 println!("in operator: {:?}", self.name);
        //                 panic!("non-upright update: {:?}", updates);
        //             }
        //         }
        //     }
        // }

        // we shouldn't be pushing progress updates at finished operators. would be a bug!
        if !(self.operator.is_some() || external_progress.iter().all(|x| x.len() == 0)) {
            println!("Operator prematurely shut down: {}", self.name);
        }
        assert!(self.operator.is_some() || external_progress.iter().all(|x| x.len() == 0));

        // TODO : re-introduce use of self.notify
        assert_eq!(external_progress.len(), self.external.len());
        assert_eq!(external_progress.len(), self.external_buffer.len());
        for (input, updates) in external_progress.iter().enumerate() {
            self.external[input].update_into_cm(updates, &mut self.external_buffer[input]);
        }

        {
            let changes = &mut self.external_buffer;
            self.operator.as_mut().map(|x| x.push_external_progress(changes));
            if changes.iter().any(|x| x.len() > 0) {
                println!("changes not consumed by {:?}", self.name);
            }
            debug_assert!(!changes.iter().any(|x| x.len() > 0));
        }
    }

    pub fn pull_pointstamps(&mut self, pointstamp_messages: &mut CountMap<(usize, usize, T)>,
                                       pointstamp_internal: &mut CountMap<(usize, usize, T)>) -> bool {

        let active = {

            ::logging::log(&::logging::SCHEDULE, ::logging::ScheduleEvent { id: self.id, is_start: true });

            let result = if let &mut Some(ref mut operator) = &mut self.operator {

                operator.pull_internal_progress(
                    &mut self.consumed_buffer[..],
                    &mut self.internal_buffer[..],
                    &mut self.produced_buffer[..],
                )
            }
            else { false };

            ::logging::log(&::logging::SCHEDULE, ::logging::ScheduleEvent { id: self.id, is_start: false });

            result
        };

        // shutting down if nothing left to do
        if self.operator.is_some() &&
           !active &&
           self.notify && // we don't track guarantees and capabilities for non-notify scopes. bug?
           self.external.iter().all(|x| x.empty()) &&
           self.internal.iter().all(|x| x.empty()) {
            //    println!("Shutting down {}", self.name);
               self.operator = None;
               self.name = format!("{}(tombstone)", self.name);
           }

        // for each output: produced messages and internal progress
        for output in 0..self.outputs {
            while let Some((time, delta)) = self.produced_buffer[output].pop() {
                for target in self.edges[output].iter() {
                    pointstamp_messages.update(&(target.index, target.port, time), delta);
                }
            }

            while let Some((time, delta)) = self.internal_buffer[output].pop() {
                let index = self.index;
                // self.internal[output].update_and(&time, delta, |&t,d| {
                    pointstamp_internal.update(&(index, output, time), delta);
                // });
            }
        }

        // for each input: consumed messages
        for input in 0..self.inputs {
            while let Some((time, delta)) = self.consumed_buffer[input].pop() {
                pointstamp_messages.update(&(self.index, input, time), -delta);
            }
        }

        return active;
    }
}
