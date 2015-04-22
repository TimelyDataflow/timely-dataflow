use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Scope};
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::*;
use communication::pact::Pipeline;
use communication::channels::ObserverHelper;
use example::builder::Graph;
use example::stream::Stream;
use example::unary::PullableHelper;


pub trait PartitionExt<'a, G: Graph+'a, D: Data, F: Fn(&D)->u64> {
    fn partition(&mut self, parts: u64, func: F) -> Vec<Stream<'a, G, D>>;
}

impl<'a, G: Graph+'a, D: Data, F: Fn(&D)->u64+'static> PartitionExt<'a, G, D, F> for Stream<'a, G, D> {
    fn partition(&mut self, parts: u64, func: F) -> Vec<Stream<'a, G, D>> {

        let (sender, receiver) = self.graph.borrow_mut().with_communicator(|x| Pipeline.connect(x));

        let mut ports = Vec::new();
        let mut registrars = Vec::new();
        for _ in 0..parts {
            let (port, registrar) = OutputPort::<G::Timestamp,D>::new();
            ports.push(port);
            registrars.push(registrar);
            // targets.push(OutputPort::<G::Timestamp,D>::new());
        }
        let scope = PartitionScope::new(receiver, ports, func);
        let index = self.graph.borrow_mut().add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);

        let mut results = Vec::new();
        for (output, registrar) in registrars.into_iter().enumerate() {
            results.push(self.clone_with(ScopeOutput(index, output as u64), registrar));
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
    pub fn new(input: P, outputs: Vec<OutputPort<T, D>>, func: F) -> PartitionScope<T, D, F, P> {
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

            for datum in data {
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
