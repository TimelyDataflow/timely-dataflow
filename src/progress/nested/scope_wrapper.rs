//! Wraps a child operator, and mediates the communication of progress information.

use std::default::Default;

use progress::frontier::{MutableAntichain, Antichain};
use progress::{Timestamp, Operate};
use progress::nested::Target;
use progress::nested::subgraph::Target::{GraphOutput, ChildInput};
use progress::count_map::CountMap;

pub struct ChildWrapper<T: Timestamp> {
    pub name:                   String,
    pub addr:                   Vec<usize>,
    pub scope:                  Option<Box<Operate<T>>>,          // the scope itself

    index:                      usize,

    pub inputs:                 usize,                       // cached information about inputs
    pub outputs:                usize,                       // cached information about outputs

    pub edges:                  Vec<Vec<Target>>,

    pub local:                  bool,                       // cached information about whether the operator is local
    pub notify:                 bool,
    pub summary:                Vec<Vec<Antichain<T::Summary>>>,     // internal path summaries (input x output)

    pub guarantees:             Vec<MutableAntichain<T>>,   // per-input:   guarantee made by parent scope in inputs
    pub capabilities:           Vec<MutableAntichain<T>>,   // per-output:  capabilities retained by scope on outputs
    pub outstanding_messages:   Vec<MutableAntichain<T>>,   // per-input:   counts of messages on each input

    internal_progress:          Vec<CountMap<T>>,         // per-output:  temp buffer used to ask about internal progress
    consumed_messages:          Vec<CountMap<T>>,         // per-input:   temp buffer used to ask about consumed messages
    produced_messages:          Vec<CountMap<T>>,         // per-output:  temp buffer used to ask about produced messages

    pub guarantee_changes:      Vec<CountMap<T>>,         // per-input:   temp storage for changes in some guarantee...
}

impl<T: Timestamp> ChildWrapper<T> {
    pub fn new(mut scope: Box<Operate<T>>, index: usize, mut path: Vec<usize>) -> ChildWrapper<T> {

        // LOGGING
        path.push(index);

        ::logging::log(&::logging::OPERATES, ::logging::OperatesEvent { addr: path.clone(), name: scope.name().to_owned() });

        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        let notify = scope.notify_me();

        let (summary, work) = scope.get_internal_summary();

        assert!(summary.len() == inputs);
        assert!(!summary.iter().any(|x| x.len() != outputs));

        let mut result = ChildWrapper {
            name:       format!("{}[{}]", scope.name(), index),
            addr:       path,
            scope:      Some(scope),
            index:      index,
            local:      local,
            inputs:     inputs,
            outputs:    outputs,
            edges:      vec![Default::default(); outputs],

            notify:     notify,
            summary:    summary,

            guarantees:             vec![Default::default(); inputs],
            capabilities:           vec![Default::default(); outputs],
            outstanding_messages:   vec![Default::default(); inputs],

            internal_progress: vec![CountMap::new(); outputs],
            consumed_messages: vec![CountMap::new(); inputs],
            produced_messages: vec![CountMap::new(); outputs],

            guarantee_changes: vec![CountMap::new(); inputs],
        };

        // TODO : Gross. Fix.
        for (index, capability) in result.capabilities.iter_mut().enumerate() {
            capability.update_iter(work[index].elements().iter().map(|x|x.clone()));
        }

        return result;
    }

    pub fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<T::Summary>>>, frontier: &mut [CountMap<T>]) {
        self.scope.as_mut().map(|scope| scope.set_external_summary(summaries, frontier));
    }


    pub fn push_pointstamps(&mut self, external_progress: &[CountMap<T>]) {

        assert!(self.scope.is_some() || external_progress.iter().all(|x| x.len() == 0));

        if self.notify && external_progress.iter().any(|x| x.len() > 0) {
            for input_port in (0..self.inputs) {
                self.guarantees[input_port]
                    .update_into_cm(&external_progress[input_port], &mut self.guarantee_changes[input_port]);
            }

            // push any changes to the frontier to the subgraph.
            if self.guarantee_changes.iter().any(|x| x.len() > 0) {
                let changes = &mut self.guarantee_changes;
                self.scope.as_mut().map(|scope| scope.push_external_progress(changes));

                // TODO : Shouldn't be necessary
                // for change in self.guarantee_changes.iter_mut() { change.clear(); }
                debug_assert!(!changes.iter().any(|x| x.len() > 0));
            }
        }
    }

    pub fn pull_pointstamps(&mut self, pointstamp_messages: &mut CountMap<(usize, usize, T)>,
                                       pointstamp_internal: &mut CountMap<(usize, usize, T)>) -> bool {

        let active = {

            ::logging::log(&::logging::SCHEDULE, ::logging::ScheduleEvent { addr: self.addr.clone(), is_start: true });

            let result = if let &mut Some(ref mut scope) = &mut self.scope {
                scope.pull_internal_progress(&mut self.internal_progress,
                                             &mut self.consumed_messages,
                                             &mut self.produced_messages)
            }
            else { false };

            ::logging::log(&::logging::SCHEDULE, ::logging::ScheduleEvent { addr: self.addr.clone(), is_start: false });

            result
        };

        // shutting down if nothing left to do
        if self.scope.is_some() &&
           !active &&
           self.notify && // we don't track guarantees and capabilities for non-notify scopes. bug?
           self.guarantees.iter().all(|guarantee| guarantee.empty()) &&
           self.capabilities.iter().all(|capability| capability.empty()) {
            //    println!("Shutting down {}", self.name);
               self.scope = None;
               self.name = format!("{}(tombstone)", self.name);
           }

        // for each output: produced messages and internal progress
        for output in (0..self.outputs) {
            while let Some((time, delta)) = self.produced_messages[output].pop() {
                for &(target, t_port) in self.edges[output].iter() {
                    pointstamp_messages.update((target, t_port, time), delta);
                }
            }

            while let Some((time, delta)) = self.internal_progress[output].pop() {
                pointstamp_internal.update(&(self.index, output, time), delta);
            }
        }

        // for each input: consumed messages
        for input in (0..self.inputs) {
            while let Some((time, delta)) = self.consumed_messages[input].pop() {
                pointstamp_messages.update(&(self.index, input, time), -delta);
            }
        }

        return active;
    }

    pub fn add_edge(&mut self, output: usize, target: Target) { self.edges[output].push(target); }

    pub fn name(&self) -> String { self.name.clone() }
}
