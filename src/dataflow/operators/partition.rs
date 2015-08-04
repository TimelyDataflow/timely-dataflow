use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Operate};
use progress::nested::Source::ChildOutput;
use progress::nested::Target::ChildInput;
use progress::count_map::CountMap;

use Data;

use dataflow::channels::pact::{ParallelizationContract, Pipeline};
use dataflow::channels::pushers::tee::Tee;
use dataflow::channels::pushers::counter::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pullers::Counter as PullCounter;

use dataflow::{Stream, Scope};

use drain::DrainExt;

pub trait Partition<G: Scope, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D2>>;
}

impl<G: Scope, D: Data, D2: Data, F: Fn(D)->(u64, D2)+'static> Partition<G, D, D2, F> for Stream<G, D> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D2>> {

        let mut scope = self.scope();

        let (sender, receiver) = Pipeline.connect(&mut scope);

        let mut targets = Vec::new();
        let mut registrars = Vec::new();
        for _ in 0..parts {
            let (target, registrar) = Tee::<G::Timestamp,D2>::new();
            targets.push(target);
            registrars.push(registrar);
        }

        let operator = Operator::new(PullCounter::new(receiver), targets, func);
        let index = scope.add_operator(operator);
        self.connect_to(ChildInput(index, 0), sender);

        let mut results = Vec::new();
        for (output, registrar) in registrars.into_iter().enumerate() {
            results.push(Stream::new(ChildOutput(index, output), registrar, scope.clone()));
        }

        results
    }
}

pub struct Operator<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    input:   PullCounter<T, D>,
    outputs: Vec<PushBuffer<T, D2, PushCounter<T, D2, Tee<T, D2>>>>,
    func:    F,
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> Operator<T, D, D2, F> {
    pub fn new(input: PullCounter<T, D>, outputs: Vec<Tee<T, D2>>, func: F) -> Operator<T, D, D2, F> {
        Operator {
            input:      input,
            outputs:    outputs.into_iter().map(|x| PushBuffer::new(PushCounter::new(x, Rc::new(RefCell::new(CountMap::new()))))).collect(),
            func:       func,
        }
    }
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> Operate<T> for Operator<T, D, D2, F> {
    fn name(&self) -> &str { "Partition" }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { self.outputs.len() }

    fn pull_internal_progress(&mut self,_internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, data)) = self.input.next() {
            let outputs = self.outputs.iter_mut();

            // TODO : This results in small sends for many parts, as sessions does the buffering
            let mut sessions: Vec<_> = outputs.map(|x| x.session(&time)).collect();

            for (part, datum) in data.drain_temp().map(&self.func) {
                sessions[part as usize].give(datum);
            }
        }

        self.input.pull_progress(&mut consumed[0]);
        for (index, output) in self.outputs.iter_mut().enumerate() {
            output.cease();
            output.inner().pull_progress(&mut produced[index]);
        }

        return false;   // no reason to keep running on Partition's account
    }

    fn notify_me(&self) -> bool { false }
}
