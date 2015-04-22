use std::default::Default;

use communication::Communicator;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, Scope};
use progress::nested::Target;
use progress::nested::subgraph::Target::{GraphOutput, ScopeInput};

use progress::count_map::CountMap;

use progress::broadcast::ProgressVec;

pub struct ScopeWrapper<T: Timestamp> {
    pub scope:                  Box<Scope<T>>,          // the scope itself

    index:                  u64,

    pub inputs:                 u64,                       // cached information about inputs
    pub outputs:                u64,                       // cached information about outputs

    pub edges:                  Vec<Vec<Target>>,

    pub notify:                 bool,
    pub summary:                Vec<Vec<Antichain<T::Summary>>>,     // internal path summaries (input x output)

    pub guarantees:             Vec<MutableAntichain<T>>,   // per-input:   guarantee made by parent scope in inputs
    pub capabilities:           Vec<MutableAntichain<T>>,   // per-output:  capabilities retained by scope on outputs
    pub outstanding_messages:   Vec<MutableAntichain<T>>,   // per-input:   counts of messages on each input

    internal_progress:      Vec<CountMap<T>>,         // per-output:  temp buffer used to ask about internal progress
    consumed_messages:      Vec<CountMap<T>>,         // per-input:   temp buffer used to ask about consumed messages
    produced_messages:      Vec<CountMap<T>>,         // per-output:  temp buffer used to ask about produced messages

    pub guarantee_changes:      Vec<CountMap<T>>,         // per-input:   temp storage for changes in some guarantee...
}

impl<T: Timestamp> ScopeWrapper<T> {
    pub fn new(scope: Box<Scope<T>>, index: u64) -> ScopeWrapper<T> {
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let mut result = ScopeWrapper {
            scope:      scope,
            index:      index,
            inputs:     inputs,
            outputs:    outputs,
            edges:      vec![Default::default(); outputs as usize],

            notify:     notify,
            summary:    Vec::new(),

            guarantees:             vec![Default::default(); inputs as usize],
            capabilities:           vec![Default::default(); outputs as usize],
            outstanding_messages:   vec![Default::default(); inputs as usize],

            internal_progress: vec![CountMap::new(); outputs as usize],
            consumed_messages: vec![CountMap::new(); inputs as usize],
            produced_messages: vec![CountMap::new(); outputs as usize],

            guarantee_changes: vec![CountMap::new(); inputs as usize],
        };

        let (summary, work) = result.scope.get_internal_summary();

        result.summary = summary;

        // TODO : Gross. Fix.
        for (index, capability) in result.capabilities.iter_mut().enumerate() {
            capability.update_iter_and(work[index].elements().iter().map(|x|x.clone()), |_, _| {});
        }

        return result;
    }

    pub fn push_pointstamps(&mut self, external_progress: &Vec<CountMap<T>>) {
        if self.notify && external_progress.iter().any(|x| x.len() > 0) {
            for input_port in (0..self.inputs as usize) {
                self.guarantees[input_port]
                    .update_into_cm(&external_progress[input_port], &mut self.guarantee_changes[input_port]);
            }

            // push any changes to the frontier to the subgraph.
            if self.guarantee_changes.iter().any(|x| x.len() > 0) {
                self.scope.push_external_progress(&mut self.guarantee_changes);

                // TODO : Shouldn't be necessary
                // for change in self.guarantee_changes.iter_mut() { change.clear(); }
                debug_assert!(!self.guarantee_changes.iter().any(|x| x.len() > 0));
            }
        }
    }

    pub fn pull_pointstamps<A: FnMut(u64, T,i64)->()>(&mut self,
                                                  pointstamp_messages: &mut ProgressVec<T>,
                                                  pointstamp_internal: &mut ProgressVec<T>,
                                                  mut output_action:   A) -> bool {

        let active = self.scope.pull_internal_progress(&mut self.internal_progress,
                                                       &mut self.consumed_messages,
                                                       &mut self.produced_messages);

        // for each output: produced messages and internal progress
        for output in (0..self.outputs as usize) {
            while let Some((time, delta)) = self.produced_messages[output].pop() {
                for &target in self.edges[output].iter() {
                    match target {
                        ScopeInput(tgt, tgt_in)   => { pointstamp_messages.push((tgt, tgt_in, time, delta)); },
                        GraphOutput(graph_output) => { output_action(graph_output, time, delta); },
                    }
                }
            }

            while let Some((time, delta)) = self.internal_progress[output as usize].pop() {
                pointstamp_internal.push((self.index, output as u64, time, delta));
            }
        }

        // for each input: consumed messages
        for input in (0..self.inputs as usize) {
            while let Some((time, delta)) = self.consumed_messages[input as usize].pop() {
                pointstamp_messages.push((self.index, input as u64, time, -delta));
            }
        }

        return active;
    }

    pub fn add_edge(&mut self, output: u64, target: Target) { self.edges[output as usize].push(target); }
}
