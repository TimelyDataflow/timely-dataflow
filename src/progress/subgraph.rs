use std::cmp::Ordering;
use std::default::Default;
use core::fmt::Show;

use std::rc::Rc;
use std::cell::RefCell;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::subgraph::Source::{GraphInput, ScopeOutput};
use progress::subgraph::Target::{GraphOutput, ScopeInput};
use progress::subgraph::Location::{SourceLoc, TargetLoc};

use progress::subgraph::Summary::{Local, Outer};
use progress::count_map::CountMap;

use progress::broadcast::{ProgressBroadcaster, Progress};
use progress::broadcast::Progress::{MessagesUpdate, FrontierUpdate};

#[derive(Eq, PartialEq, Hash, Copy, Clone, Show)]
pub enum Source {
    GraphInput(u64),           // from outer scope
    ScopeOutput(u64, u64),    // (scope, port) may have interesting connectivity
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Show)]
pub enum Target {
    GraphOutput(u64),          // to outer scope
    ScopeInput(u64, u64),     // (scope, port) may have interesting connectivity
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Show)]
pub enum Location {
    SourceLoc(Source),
    TargetLoc(Target),
}


impl<TOuter: Timestamp, TInner: Timestamp> Timestamp for (TOuter, TInner) { }

#[derive(Copy, Clone, Eq, PartialEq, Show)]
pub enum Summary<S, T> {
    Local(T),    // reachable within scope, after some iterations.
    Outer(S, T), // unreachable within scope, reachable through outer scope and some iterations.
}

impl<S, T: Default> Default for Summary<S, T> {
    fn default() -> Summary<S, T> { Local(Default::default()) }
}

impl<S:PartialOrd+Copy, T:PartialOrd+Copy> PartialOrd for Summary<S, T> {
    fn partial_cmp(&self, other: &Summary<S, T>) -> Option<Ordering> {
        match *self {
            Local(iters) => {
                match *other {
                    Local(iters2) => iters.partial_cmp(&iters2),
                    _ => Some(Ordering::Less),
                }
            },
            Outer(s1, iters) => {
                match *other {
                    Outer(s2, iters2) => if s1.eq(&s2) { iters.partial_cmp(&iters2) }
                                         else          { s1.partial_cmp(&s2) },
                    _ => Some(Ordering::Greater),
                }
            },
        }
    }
}

impl<TOuter, SOuter, TInner, SInner>
PathSummary<(TOuter, TInner)>
for Summary<SOuter, SInner>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
{
    // this makes sense for a total order, but less clear for a partial order.
    fn results_in(&self, src: &(TOuter, TInner)) -> (TOuter, TInner) {
        let &(outer, inner) = src;
        match *self {
            Local(iters)              => (outer, iters.results_in(&inner)),
            Outer(ref summary, iters) => (summary.results_in(&outer), iters.results_in(&Default::default())),
        }
    }
    fn followed_by(&self, other: &Summary<SOuter, SInner>) -> Summary<SOuter, SInner>
    {
        match (*self, *other) {
            (Local(inner1), Local(inner2))             => Local(inner1.followed_by(&inner2)),
            (Local(_), Outer(_, _))                    => *other,
            (Outer(outer1, inner1), Local(inner2))     => Outer(outer1, inner1.followed_by(&inner2)),
            (Outer(outer1, _), Outer(outer2, inner2))  => Outer(outer1.followed_by(&outer2), inner2),
        }
        // match *self {
        //     Local(iter) => {
        //         match *other {
        //             Local(iter2) => Local(iter.followed_by(&iter2)),
        //             Outer(_, _) => *other,
        //         }
        //     },
        //     Outer(summary, iter) => {
        //         match *other {
        //             Local(iter2) => Outer(summary, iter.followed_by(&iter2)),
        //             Outer(sum2, iter2) => Outer(summary.followed_by(&sum2), iter2),
        //         }
        //     },
        // }
    }
}

/*
#[deriving(Default)]
pub struct PerScopeState<T: Timestamp, S: PathSummary<T>>
{
    inputs:                 uint,                       // cached information about inputs
    outputs:                uint,                       // cached information about outputs

    scope:                  Box<Scope<T, S>>,           // the scope itself

    summary:                Vec<Vec<Antichain<S>>>,     // internal path summaries (input x output)
    guarantees:             Vec<MutableAntichain<T>>,   // per-input:   guarantee made by parent scope in inputs
    capabilities:           Vec<MutableAntichain<T>>,   // per-output:  capabilities retained by scope on outputs
    outstanding_messages:   Vec<MutableAntichain<T>>,   // per-input:   counts of messages on each input

    progress:               Vec<Vec<(T, i64)>>,         // per-output:  temp buffer used to ask about progress
    consumed:               Vec<Vec<(T, i64)>>,         // per-input:   temp buffer used to ask about consumed messages
    produced:               Vec<Vec<(T, i64)>>,         // per-output:  temp buffer used to ask about produced messages

    guarantee_changes:      Vec<Vec<(T, i64)>>,         // per-input:   temp storage for changes in some guarantee...

    source_counts:          Vec<Vec<(T, i64)>>,         // per-output:  counts of pointstamp changes at sources
    target_counts:          Vec<Vec<(T, i64)>>,         // per-input:   counts of pointstamp changes at targets

    target_pushed:          Vec<Vec<(T, i64)>>,         // per-input:   counts of pointstamp changes pushed to targets
}

*/

#[derive(Default)]
pub struct SubscopeState<TTime:Timestamp, TSummary> {
    summary:                Vec<Vec<Antichain<TSummary>>>,  // internal path summaries
    guarantees:             Vec<MutableAntichain<TTime>>,   // guarantee made by parent scope
    capabilities:           Vec<MutableAntichain<TTime>>,   // capabilities retained by scope
    outstanding_messages:   Vec<MutableAntichain<TTime>>,
}

impl<TTime:Timestamp, TSummary> SubscopeState<TTime, TSummary> {
    pub fn new(inputs: u64, outputs: u64, summary: Vec<Vec<Antichain<TSummary>>>) -> SubscopeState<TTime, TSummary> {
        SubscopeState {
            summary:                summary,
            guarantees:             (0..inputs).map(|_| Default::default()).collect(),
            capabilities:           (0..outputs).map(|_| Default::default()).collect(),
            outstanding_messages:   (0..inputs).map(|_| Default::default()).collect(),
        }
    }
}

#[derive(Default)]
pub struct SubscopeBuffers<TTime:Timestamp> {
    progress:        Vec<Vec<(TTime, i64)>>,            // buffers to use soliciting progress information
    consumed:        Vec<Vec<(TTime, i64)>>,            // buffers to use soliciting progress information
    produced:        Vec<Vec<(TTime, i64)>>,            // buffers to use soliciting progress information

    guarantee_changes:      Vec<Vec<(TTime, i64)>>,     // TODO : Can't remember...
}


impl<TTime:Timestamp> SubscopeBuffers<TTime> {
    pub fn new(inputs: u64, outputs: u64) -> SubscopeBuffers<TTime> {
        SubscopeBuffers {
            progress:    (0..outputs).map(|_| Vec::new()).collect(),
            consumed:    (0..inputs).map(|_| Vec::new()).collect(),
            produced:    (0..outputs).map(|_| Vec::new()).collect(),

            guarantee_changes:  (0..inputs).map(|_| Vec::new()).collect(),
        }
    }
}

#[derive(Default)]
pub struct PointstampCounter<T:Timestamp> {
    pub source_counts:  Vec<Vec<Vec<(T, i64)>>>,    // timestamp updates indexed by (scope, output)
    pub target_counts:  Vec<Vec<Vec<(T, i64)>>>,    // timestamp updates indexed by (scope, input)
    pub input_counts:   Vec<Vec<(T, i64)>>,         // timestamp updates indexed by input_port
    pub target_pushed:  Vec<Vec<Vec<(T, i64)>>>,    // pushed updates indexed by (scope, input)
    pub output_pushed:  Vec<Vec<(T, i64)>>,         // pushed updates indexed by output_port
}

impl<T:Timestamp> PointstampCounter<T> {
    //#[inline(always)]
    pub fn update_target(&mut self, target: Target, time: &T, value: i64) {
        if let ScopeInput(scope, input) = target { self.target_counts[scope as usize][input as usize].update(time, value); }
        else                                     { println!("lolwut?"); } // no graph outputs as poinstamps
    }

    pub fn update_source(&mut self, source: Source, time: &T, value: i64) {
        match source {
            ScopeOutput(scope, output) => { self.source_counts[scope as usize][output as usize].update(time, value); },
            GraphInput(input)          => { self.input_counts[input as usize].update(time, value); },
        }
    }
    pub fn clear_pushed(&mut self) {
        for vec in self.target_pushed.iter_mut() { for map in vec.iter_mut() { map.clear(); } }
        for map in self.output_pushed.iter_mut() { map.clear(); }
    }
}

#[derive(Default)]
pub struct Subgraph<TOuter:Timestamp, SOuter, TInner:Timestamp, SInner, Broadcaster: ProgressBroadcaster<(TOuter, TInner)>> {

    pub name:               String,                     // a helpful name
    pub index:              u64,                        // a useful integer

    //default_time:           (TOuter, TInner),
    default_summary:        Summary<SOuter, SInner>,    // default summary to use for something TODO: figure out what.

    inputs:                 u64,                        // number inputs into the scope
    outputs:                u64,                        // number outputs from the scope

    scope_edges:            Vec<Vec<Vec<Target>>>,      // edges as list of Targets for each (scope, port).
    input_edges:            Vec<Vec<Target>>,           // edges as list of Targets for each input_port.

    external_summaries:     Vec<Vec<Antichain<SOuter>>>,// path summaries from output -> input (TODO: Check) using any edges

    // maps from (scope, output), (scope, input) and (input) to respective Vec<(target, antichain)> lists
    // TODO: sparsify complete_summaries to contain only paths which avoid their target scopes.
    source_summaries:       Vec<Vec<Vec<(Target, Antichain<Summary<SOuter, SInner>>)>>>,
    target_summaries:       Vec<Vec<Vec<(Target, Antichain<Summary<SOuter, SInner>>)>>>,
    input_summaries:        Vec<Vec<(Target, Antichain<Summary<SOuter, SInner>>)>>,

    // state reflecting work in and promises made to external scope.
    external_capability:    Vec<MutableAntichain<TOuter>>,
    external_guarantee:     Vec<MutableAntichain<TOuter>>,

    // all of the subscopes, and their internal_summaries (ss[g][i][o] = ss[g].i_s[i][o])
    pub subscopes:          Vec<Box<Scope<(TOuter, TInner), Summary<SOuter, SInner>>>>,
    subscope_state:         Vec<SubscopeState<(TOuter, TInner), Summary<SOuter, SInner>>>,
    subscope_buffers:       Vec<SubscopeBuffers<(TOuter, TInner)>>,

    notify:                 Vec<bool>,                          // cached subscopes[i].notify_me() (avoids virtual calls)

    input_messages:         Vec<Rc<RefCell<Vec<((TOuter, TInner), i64)>>>>,

    pointstamps:            PointstampCounter<(TOuter, TInner)>,
    pointstamp_updates:     Vec<Progress<(TOuter, TInner)>>,    // buffer to stash pointstamp updates.

    pub broadcaster:        Broadcaster,
}


impl<TOuter, SOuter, TInner, SInner, Bcast>
Scope<TOuter, SOuter>
for Subgraph<TOuter, SOuter, TInner, SInner, Bcast>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
      Bcast:  ProgressBroadcaster<(TOuter, TInner)>
{
    fn name(&self) -> String { self.name.clone() }
    fn inputs(&self)  -> u64 { self.inputs }
    fn outputs(&self) -> u64 { self.outputs }

    // produces (in -> out) summaries using only edges internal to the vertex.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<SOuter>>>, Vec<Vec<(TOuter, i64)>>) {
        // seal subscopes; prepare per-scope state/buffers
        for index in range(0, self.subscopes.len()) {
            let (summary, work) = self.subscopes[index].get_internal_summary();

            let inputs = self.subscopes[index].inputs();
            let outputs = self.subscopes[index].outputs();

            let mut new_state = SubscopeState::new(inputs, outputs, summary);

            // install initial capabilities
            for output in range(0, outputs){
                new_state.capabilities[output as usize].update_iter_and(work[output as usize].iter().map(|&x|x), |_, _| {});
            }

            self.notify.push(self.subscopes[index].notify_me());
            self.subscope_state.push(new_state);
            self.subscope_buffers.push(SubscopeBuffers::new(inputs, outputs));

            // initialize storage for vector-based source and target path summaries.
            self.source_summaries.push(range(0, outputs).map(|_| Vec::new()).collect());
            self.target_summaries.push(range(0, inputs).map(|_| Vec::new()).collect());

            self.pointstamps.target_pushed.push(range(0, inputs).map(|_| Default::default()).collect());
            self.pointstamps.target_counts.push(range(0, inputs).map(|_| Default::default()).collect());
            self.pointstamps.source_counts.push(range(0, outputs).map(|_| Default::default()).collect());

            // take capabilities as pointstamps
            for output in (0..outputs) {
                for time in self.subscope_state[index].capabilities[output as usize].elements.iter(){
                    self.pointstamps.update_source(ScopeOutput(index as u64, output), time, 1);
                }
            }
        }

        // initialize space for input -> Vec<(Target, Antichain) mapping.
        self.input_summaries = range(0, self.inputs()).map(|_| Vec::new()).collect();

        self.pointstamps.input_counts = range(0, self.inputs()).map(|_| Default::default()).collect();
        self.pointstamps.output_pushed = range(0, self.outputs()).map(|_| Default::default()).collect();

        self.external_summaries = range(0, self.outputs()).map(|_| range(0, self.inputs()).map(|_| Default::default()).collect())
                                                                                          .collect();

        // TODO: Explain better.
        self.set_summaries();

        self.push_pointstamps_to_targets();

        // TODO: WTF is this all about? Who wrote this? Me...
        let mut work: Vec<_> = range(0, self.outputs()).map(|_| Vec::new()).collect();
        for (output, map) in work.iter_mut().enumerate() {
            for &(key, val) in self.pointstamps.output_pushed[output].elements().iter() {
                map.push((key.0, val));
            }
        }

        let mut summaries: Vec<Vec<_>> = (0..self.inputs()).map(|_| (0..self.outputs()).map(|_| Antichain::new())
                                                                                       .collect()).collect();
        for input in range(0, self.inputs()) {
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
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<SOuter>>>, frontier: &Vec<Vec<(TOuter, i64)>>) -> () {
        self.external_summaries = summaries;
        self.set_summaries();        // now sort out complete reachability internally...

        // change frontier to local times; introduce as pointstamps
        for graph_input in range(0, self.inputs()) {
            for &(time, val) in frontier[graph_input as usize].iter() {
                self.pointstamps.update_source(GraphInput(graph_input), &(time, Default::default()), val);
            }
        }

        // identify all capabilities expressed locally
        for scope in (0..self.subscopes.len()) {
            for output in (0..self.subscopes[scope].outputs()) {
                for time in self.subscope_state[scope].capabilities[output as usize].elements.iter() {
                    self.pointstamps.update_source(ScopeOutput(scope as u64, output), time, 1);
                }
            }
        }

        self.push_pointstamps_to_targets();

        // for each subgraph, compute summaries based on external edges.
        for subscope in (0..self.subscopes.len()) {
            let changes = &mut self.subscope_buffers[subscope].guarantee_changes;
            if self.subscopes[subscope].notify_me() {
                for input_port in range(0, changes.len()) {
                    self.subscope_state[subscope]
                        .guarantees[input_port]
                        .update_into_cm(&self.pointstamps.target_pushed[subscope][input_port], &mut changes[input_port]);
                }
            }

            let inputs = self.subscopes[subscope].inputs();
            let outputs = self.subscopes[subscope].outputs();

            let mut summaries: Vec<Vec<_>> = range(0, outputs).map(|_| range(0, inputs).map(|_| Antichain::new()).collect())
                                                                                       .collect();

            for output in (0..summaries.len()) {
                for &(target, ref antichain) in self.source_summaries[subscope][output].iter() {
                    if let ScopeInput(target_scope, target_input) = target {
                        if target_scope == subscope as u64 { summaries[output][target_input as usize] = antichain.clone()}
                    }
                }
            }

            self.subscopes[subscope].set_external_summary(summaries, changes);
            for change in changes.iter_mut() { change.clear(); }
        }

        self.pointstamps.clear_pushed();
    }

    // information for the scope about progress in the outside world (updates to the input frontiers)
    // important to push this information on to subscopes.
    fn push_external_progress(&mut self, frontier_progress: &Vec<Vec<(TOuter, i64)>>) -> () {
        // transform into pointstamps to use push_progress_to_target().
        for (input, progress) in frontier_progress.iter().enumerate() {
            for &(time, val) in progress.iter() {
                self.pointstamps.update_source(GraphInput(input as u64), &(time, Default::default()), val);
            }
        }

        self.push_pointstamps_to_targets();

        // consider pushing to each nested scope in turn.
        for (index, scope) in self.subscopes.iter_mut().enumerate() {
            if self.notify[index] {
                let changes = &mut self.subscope_buffers[index].guarantee_changes;

                for input_port in range(0, changes.len()) {
                    self.subscope_state[index].guarantees[input_port]
                        .update_into_cm(&self.pointstamps.target_pushed[index][input_port], &mut changes[input_port]);
                }

                // push any changes to the frontier to the subgraph.
                if changes.iter().any(|x| x.len() > 0) {
                    scope.push_external_progress(changes);
                    for change in changes.iter_mut() { change.clear(); }
                }
            }
        }

        self.pointstamps.clear_pushed();
    }

    // information from the vertex about its progress (updates to the output frontiers, recv'd and sent message counts)
    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(TOuter, i64)>>,
                                         messages_consumed: &mut Vec<Vec<(TOuter, i64)>>,
                                         messages_produced: &mut Vec<Vec<(TOuter, i64)>>) -> bool
    {
        // should be false when there is nothing left to do
        let mut active = false;

        // Step 1: handle messages introduced through each graph input
        for input in (0..self.inputs) {
            for (time, delta) in self.input_messages[input as usize].borrow_mut().drain() {
                messages_consumed[input as usize].push((time.0, delta));
                for &target in self.input_edges[input as usize].iter() {
                    match target {
                        ScopeInput(subgraph, subgraph_input) => {
                            self.pointstamp_updates.push(MessagesUpdate(subgraph, subgraph_input, time, delta));
                        },
                        GraphOutput(graph_output) => {
                            messages_produced[graph_output as usize].push((time.0, delta));
                        },
                    }
                }
            }
        }

        // Step 2: pull_internal_progress from subscopes.
        for (index, scope) in self.subscopes.iter_mut().enumerate()
        {
            let buffers = &mut self.subscope_buffers[index];
            let subactive = scope.pull_internal_progress(&mut buffers.progress, &mut buffers.consumed, &mut buffers.produced);
            if subactive { active = true; } // println!("Remaining active due to {}", scope.name());

            for output in (0..buffers.produced.len()) {

                // Step 2a: handle produced messages!
                for (time, delta) in buffers.produced[output].drain() {
                    for &target in self.scope_edges[index][output].iter() {
                        match target {
                            ScopeInput(target_scope, target_port) => {
                                self.pointstamp_updates.push(MessagesUpdate(target_scope, target_port, time, delta));
                            },
                            GraphOutput(graph_output) => {
                                messages_produced[graph_output as usize].push((time.0, delta));
                            },
                        }
                    }
                }

                // Step 2b: handle progress updates!
                for (time, delta) in buffers.progress[output as usize].drain() {
                    self.pointstamp_updates.push(FrontierUpdate(index as u64, output as u64, time, delta));
                }
            }

            // Step 2c: handle consumed messages.
            for input in range(0, buffers.consumed.len()) {
                for (time, delta) in buffers.consumed[input as usize].drain() {
                    self.pointstamp_updates.push(MessagesUpdate(index as u64, input as u64, time, -delta));
                }
            }
        }

        // Intermission: exchange pointstamp updates, and then move them to the pointstamps structure.
        self.broadcaster.send_and_recv(&mut self.pointstamp_updates);
        {
            let pointstamps = &mut self.pointstamps;
            for update in self.pointstamp_updates.drain() {
                match update {
                    MessagesUpdate(scope, input, time, delta) => {
                        self.subscope_state[scope as usize].outstanding_messages[input as usize].update_and(&time, delta, |time, delta| {
                            pointstamps.update_target(ScopeInput(scope, input), time, delta);
                        })
                    },
                    FrontierUpdate(scope, output, time, delta) => {
                        self.subscope_state[scope as usize].capabilities[output as usize].update_and(&time, delta, |time, delta| {
                            pointstamps.update_source(ScopeOutput(scope, output), time, delta);
                        });
                    },
                }
            }
        }

        self.push_pointstamps_to_targets();     // moves self.pointstamps to self.pointstamps.pushed, differentiated by target.


        // Step 3: push any progress to each target subgraph ...
        for (index, scope) in self.subscopes.iter_mut().enumerate() {
            if self.notify[index] {
                let changes = &mut self.subscope_buffers[index].guarantee_changes;

                for input_port in range(0, changes.len()) {
                    self.subscope_state[index]
                        .guarantees[input_port]
                        .update_into_cm(&self.pointstamps.target_pushed[index][input_port], &mut changes[input_port]);
                }

                // push any changes to the frontier to the subgraph.
                if changes.iter().any(|x| x.len() > 0) {
                    scope.push_external_progress(changes);
                    for change in changes.iter_mut() { change.clear(); }
                }
            }
        }

        // Step 4: push progress to each graph output ...
        for output in (0..self.outputs) {
            // prep an iterator which extracts the first field of the time
            let updates = self.pointstamps.output_pushed[output as usize].iter().map(|&(time, val)| (time.0, val));
            self.external_capability[output as usize].update_iter_and(updates, |time,val| { frontier_progress[output as usize].update(time, val); });
        }

        // pointstamps should be cleared in push_to_targets()
        self.pointstamps.clear_pushed();

        for scope in range(0, self.subscopes.len()) {
            if self.subscope_state[scope].outstanding_messages.iter().any(|x| x.elements.len() > 0) {
                active = true;
                // println!("NOT DONE: due to Scope({}): outstanding messages", self.subscopes[scope].name());
                // for input in range(0, self.subscopes[scope].inputs())
                // {
                //     println!("\tspecifically: input[{}]: {}", input, self.subscope_state[scope].outstanding_messages[input]);
                // }
            }
            if self.subscope_state[scope].capabilities.iter().any(|x| x.elements.len() > 0) {
                active = true;
                // println!("NOT DONE: due to Scope({}): outstanding capabilities",  self.subscopes[scope].name());
                // for output in range(0, self.subscopes[scope].outputs())
                // {
                //     println!("\tspecifically: output[{}]: {}", output, self.subscope_state[scope].capabilities[output]);
                // }
            }
        }

        return active;
    }
}


impl<TOuter, SOuter, TInner, SInner, Bcast>
Graph<(TOuter, TInner), Summary<SOuter, SInner>>
for Rc<RefCell<Subgraph<TOuter, SOuter, TInner, SInner, Bcast>>>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
      Bcast:  ProgressBroadcaster<(TOuter, TInner)>
{
    fn connect(&mut self, source: Source, target: Target) { self.borrow_mut().connect(source, target); }

    fn add_boxed_scope(&mut self, scope: Box<Scope<(TOuter, TInner), Summary<SOuter, SInner>>>) -> u64 {
        let mut borrow = self.borrow_mut();
        borrow.subscopes.push(scope);
        return borrow.subscopes.len() as u64 - 1;
    }

    fn as_box(&self) -> Box<Graph<(TOuter, TInner), Summary<SOuter, SInner>>> { Box::new(self.clone()) }
}

impl<TOuter, SOuter, TInner, SInner, Bcast>
Subgraph<TOuter, SOuter, TInner, SInner, Bcast>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
      Bcast:  ProgressBroadcaster<(TOuter, TInner)>
{
    fn push_pointstamps_to_targets(&mut self) -> () {
        for index in (0..self.subscopes.len()) {
            for input in (0..self.pointstamps.target_counts[index].len()) {
                for (time, value) in self.pointstamps.target_counts[index][input as usize].drain() {
                    for &(target, ref antichain) in self.target_summaries[index][input as usize].iter() {
                        for summary in antichain.elements.iter() {
                            match target {
                                ScopeInput(scope, input) => &mut self.pointstamps.target_pushed[scope as usize][input as usize],
                                GraphOutput(output)      => &mut self.pointstamps.output_pushed[output as usize],
                            }.update(&summary.results_in(&time), value);
                        }
                    }
                }
            }

            for output in (0..self.pointstamps.source_counts[index].len()) {
                for (time, value) in self.pointstamps.source_counts[index][output as usize].drain() {
                    for &(target, ref antichain) in self.source_summaries[index][output as usize].iter() {
                        for summary in antichain.elements.iter() {
                            match target {
                                ScopeInput(scope, input) => &mut self.pointstamps.target_pushed[scope as usize][input as usize],
                                GraphOutput(output)      => &mut self.pointstamps.output_pushed[output as usize],
                            }.update(&summary.results_in(&time), value);
                        }
                    }
                }
            }
        }

        for input in (0..self.inputs) {                                          // for each graph inputs ...
            for (time, value) in self.pointstamps.input_counts[input as usize].drain() {         // for each update at GraphInput(input)...
                for &(target, ref antichain) in self.input_summaries[input as usize].iter() {    // for each target it can reach ...
                    for summary in antichain.elements.iter() {                          // ... do stuff.
                        match target {
                            ScopeInput(scope, input) => &mut self.pointstamps.target_pushed[scope as usize][input as usize],
                            GraphOutput(output)      => &mut self.pointstamps.output_pushed[output as usize],
                        }.update(&summary.results_in(&time), value);
                    }
                }
            }
        }
    }

    // Repeatedly takes edges (source, target), finds (target, source') connections,
    // expands based on (source', target') summaries.
    // Only considers targets satisfying the supplied predicate.
    fn set_summaries(&mut self) -> () {
        for scope in (0..self.subscopes.len()) {
            for output in (0..self.subscopes[scope].outputs()) {
                self.source_summaries[scope][output as usize].clear();
                for &target in self.scope_edges[scope][output as usize].iter() {
                    if match target { ScopeInput(t, _) => self.subscopes[t as usize].notify_me(), _ => true } {
                        self.source_summaries[scope][output as usize].push((target, Antichain::from_elem(self.default_summary)));
                    }
                }
            }
        }

        // load up edges from graph inputs
        for input in (0..self.inputs) {
            self.input_summaries[input as usize].clear();
            for &target in self.input_edges[input as usize].iter() {
                if match target { ScopeInput(t, _) => self.subscopes[t as usize].notify_me(), _ => true } {
                    self.input_summaries[input as usize].push((target, Antichain::from_elem(self.default_summary)));
                }
            }
        }

        let mut done = false;
        while !done {
            done = true;

            // process edges from scope outputs ...
            for scope in (0..self.subscopes.len()) {                                       // for each scope
                for output in (0..self.subscopes[scope].outputs()) {                       // for each output
                    for target in self.scope_edges[scope][output as usize].iter() {                      // for each edge target
                        let next_sources = self.target_to_sources(target);
                        for &(next_source, next_summary) in next_sources.iter() {               // for each source it reaches
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
        for scope in (0..self.subscopes.len()) {
            for input in (0..self.subscopes[scope].inputs()) {
                self.target_summaries[scope][input as usize].clear();
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

    fn target_to_sources(&self, target: &Target) -> Vec<(Source, Summary<SOuter, SInner>)> {
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
                for i in (0..self.subscopes[graph as usize].outputs()) {
                    for &summary in self.subscope_state[graph as usize].summary[port as usize][i as usize].elements.iter() {
                        result.push((ScopeOutput(graph, i), summary));
                    }
                }
            }
        }

        result
    }

    pub fn new_input(&mut self, shared_counts: Rc<RefCell<Vec<((TOuter, TInner), i64)>>>) -> u64 {
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
            ScopeOutput(scope, index) => {
                while (self.scope_edges.len() as u64) < (scope + 1)        { self.scope_edges.push(Vec::new()); }
                while (self.scope_edges[scope as usize].len() as u64) < (index + 1) { self.scope_edges[scope as usize].push(Vec::new()); }
                self.scope_edges[scope as usize][index as usize].push(target);
            },
            GraphInput(input) => {
                while (self.input_edges.len() as u64) < (input + 1)        { self.input_edges.push(Vec::new()); }
                self.input_edges[input as usize].push(target);
            },
        }
    }
}

pub fn new_graph<T, S, B>(broadcaster: B) -> Rc<RefCell<Subgraph<(), (), T, S, B>>>
where T: Timestamp, S: PathSummary<S>, B: ProgressBroadcaster<((), T)>
{
    let mut result: Subgraph<(), (), T, S, B> = Default::default();
    result.broadcaster = broadcaster;
    return Rc::new(RefCell::new(result));
}

fn try_to_add_summary<S>(vector: &mut Vec<(Target, Antichain<S>)>, target: Target, summary: S) -> bool
where S: PartialOrd+Eq+Copy+Show
{
    for &mut (ref t, ref mut antichain) in vector.iter_mut() { if target.eq(t) { return antichain.insert(summary); } }
    vector.push((target, Antichain::from_elem(summary)));
    return true;
}
