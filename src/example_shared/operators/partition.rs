use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Scope};
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communicator::Data;
use communicator::pact::{ParallelizationContract, Pipeline};
use communicator::Pullable;
use communicator::pullable::Counter as PullableCounter;
use observer::{Tee, Extensions};
use observer::Counter as ObserverCounter;

use example_shared::*;

use drain::DrainExt;

pub trait PartitionExt<G: GraphBuilder, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D2>>;
}

impl<G: GraphBuilder, D: Data, D2: Data, F: Fn(D)->(u64, D2)+'static> PartitionExt<G, D, D2, F> for Stream<G, D> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D2>> {

        let mut builder = self.builder();

        let (sender, receiver) = Pipeline.connect(&mut builder);

        let mut targets = Vec::new();
        let mut registrars = Vec::new();
        for _ in 0..parts {
            let (target, registrar) = Tee::<G::Timestamp,D2>::new();
            targets.push(target);
            registrars.push(registrar);
        }

        let scope = PartitionScope::new(receiver, targets, func);
        let index = builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);

        let mut results = Vec::new();
        for (output, registrar) in registrars.into_iter().enumerate() {
            results.push(Stream::new(ScopeOutput(index, output as u64), registrar, builder.clone()));
        }

        results
    }
}

pub struct PartitionScope<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2), P: Pullable<T, D>> {
    input:   PullableCounter<T, D, P>,
    outputs: Vec<ObserverCounter<Tee<T, D2>>>,
    func:    F,
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2), P: Pullable<T, D>> PartitionScope<T, D, D2, F, P> {
    pub fn new(input: P, outputs: Vec<Tee<T, D2>>, func: F) -> PartitionScope<T, D, D2, F, P> {
        PartitionScope {
            input:      PullableCounter::new(input),
            outputs:    outputs.into_iter().map(|x| ObserverCounter::new(x, Rc::new(RefCell::new(CountMap::new())))).collect(),
            func:       func,
        }
    }
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2), P: Pullable<T, D>> Scope<T> for PartitionScope<T, D, D2, F, P> {
    fn name(&self) -> String { format!("Partition") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { self.outputs.len() as u64 }

    fn pull_internal_progress(&mut self,_internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, data)) = self.input.pull() {
            let outputs = self.outputs.iter_mut();

            // TODO : This results in small sends for many parts, as sessions does the buffering
            let mut sessions: Vec<_> = outputs.map(|x| x.session(&time)).collect();

            for (part, datum) in data.take().drain_temp().map(&self.func) {
                sessions[part as usize].give(datum);
            }
        }

        self.input.pull_progress(&mut consumed[0]);
        for (index, output) in self.outputs.iter_mut().enumerate() {
            output.pull_progress(&mut produced[index]);
        }

        return false;   // no reason to keep running on Partition's account
    }

    fn notify_me(&self) -> bool { false }
}
