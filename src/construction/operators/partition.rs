use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Operate};
use progress::nested::Source::ChildOutput;
use progress::nested::Target::ChildInput;
use progress::count_map::CountMap;

// use communication::{Data, Pullable};
// use fabric::{Pull};
use ::Data;
// use communication::message::Message;
use communication::pact::{ParallelizationContract, Pipeline};
use communication::observer::Tee;
use communication::observer::Counter as ObserverCounter;
use communication::observer::buffer::Buffer as ObserverBuffer;
use communication::pullable::Counter as PullableCounter;

use construction::{Stream, GraphBuilder};

use drain::DrainExt;

pub trait Extension<G: GraphBuilder, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    fn partition(&self, parts: u64, func: F) -> Vec<Stream<G, D2>>;
}

impl<G: GraphBuilder, D: Data, D2: Data, F: Fn(D)->(u64, D2)+'static> Extension<G, D, D2, F> for Stream<G, D> {
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

        let operator = Operator::new(PullableCounter::new(receiver), targets, func);
        let index = builder.add_operator(operator);
        self.connect_to(ChildInput(index, 0), sender);

        let mut results = Vec::new();
        for (output, registrar) in registrars.into_iter().enumerate() {
            results.push(Stream::new(ChildOutput(index, output), registrar, builder.clone()));
        }

        results
    }
}

pub struct Operator<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    input:   PullableCounter<T, D>,
    outputs: Vec<ObserverBuffer<T, D2, ObserverCounter<T, D2, Tee<T, D2>>>>,
    func:    F,
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> Operator<T, D, D2, F> {
    pub fn new(input: PullableCounter<T, D>, outputs: Vec<Tee<T, D2>>, func: F) -> Operator<T, D, D2, F> {
        Operator {
            input:      input,
            outputs:    outputs.into_iter().map(|x| ObserverBuffer::new(ObserverCounter::new(x, Rc::new(RefCell::new(CountMap::new()))))).collect(),
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
