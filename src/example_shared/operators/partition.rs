use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Scope};
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::*;
use communication::pact::{Pipeline, PactPullable};
use communication::channels::ObserverHelper;

use example_shared::*;
use example_shared::operators::unary::PullableHelper;

use drain::DrainExt;

pub trait PartitionExt<G: GraphBuilder, D: Data, F: Fn(&D)->u64> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D>>;
}

impl<G: GraphBuilder, D: Data, F: Fn(&D)->u64+'static> PartitionExt<G, D, F> for Stream<G, D> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D>> {

        let mut builder = self.builder();

        let (sender, receiver) = Pipeline.connect(&mut builder);

        let mut targets = Vec::new();
        let mut registrars = Vec::new();
        for _ in 0..parts {
            let (target, registrar) = OutputPort::<G::Timestamp,D>::new();
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

pub struct PartitionScope<T:Timestamp, D: Data, F: Fn(&D)->u64, P: Pullable<(T, Vec<D>)>> {
    input:   PullableHelper<T, D, P>,
    outputs: Vec<ObserverHelper<OutputPort<T, D>>>,
    func:    F,
}

impl<T:Timestamp, D: Data, F: Fn(&D)->u64, P: Pullable<(T, Vec<D>)>> PartitionScope<T, D, F, P> {
    pub fn new(input: PactPullable<T, D, P>, outputs: Vec<OutputPort<T, D>>, func: F) -> PartitionScope<T, D, F, P> {
        PartitionScope {
            input:      PullableHelper::new(input),
            outputs:    outputs.into_iter().map(|x| ObserverHelper::new(x, Rc::new(RefCell::new(CountMap::new())))).collect(),
            func:       func,
        }
    }
}

impl<T:Timestamp, D: Data, F: Fn(&D)->u64, P: Pullable<(T, Vec<D>)>> Scope<T> for PartitionScope<T, D, F, P> {
    fn name(&self) -> String { format!("Partition") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { self.outputs.len() as u64 }

    fn pull_internal_progress(&mut self,_internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, data)) = self.input.pull() {
            let outputs = self.outputs.iter_mut();
            let mut sessions: Vec<_> = outputs.map(|x| x.session(&time)).collect();

            for datum in data.drain_temp() {
                let output = (self.func)(&datum);
                sessions[output as usize].give(datum);
            }
        }

        self.input.pull_progress(&mut consumed[0]);
        for (index, output) in self.outputs.iter_mut().enumerate() {
            output.pull_progress(&mut produced[index]);
        }

        return false;   // no reason to keep running on Concat's account
    }

    fn notify_me(&self) -> bool { false }
}
