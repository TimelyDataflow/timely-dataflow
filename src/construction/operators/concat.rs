use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Scope};
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::Data;
use communication::observer::{Counter, Tee};

use construction::{Stream, GraphBuilder};


pub trait ConcatExt<G: GraphBuilder, D: Data> {
    fn concat(&self, &Stream<G, D>) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data> ConcatExt<G, D> for Stream<G, D> {
    fn concat(&self, other: &Stream<G, D>) -> Stream<G, D> {

        let (outputs, receiver) = Tee::<G::Timestamp, D>::new();
        let consumed = vec![Rc::new(RefCell::new(CountMap::new())),
                            Rc::new(RefCell::new(CountMap::new()))];

        let builder = self.builder();

        let index = builder.add_scope(ConcatScope { consumed: consumed.clone() });

        self.connect_to(ScopeInput(index, 0), Counter::new(outputs.clone(), consumed[0].clone()));
        other.connect_to(ScopeInput(index, 1), Counter::new(outputs, consumed[1].clone()));

        Stream::new(ScopeOutput(index, 0), receiver, builder)
    }
}

pub trait ConcatVecExt<G: GraphBuilder, D: Data> {
    fn concatenate(&self, Vec<Stream<G, D>>) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data> ConcatVecExt<G, D> for G {
    fn concatenate(&self, sources: Vec<Stream<G, D>>) -> Stream<G, D> {

        if sources.len() == 0 { panic!("must pass at least one stream to concat"); }

        let (outputs, registrar) = Tee::<G::Timestamp, D>::new();
        let mut consumed = Vec::new();
        for _ in 0..sources.len() { consumed.push(Rc::new(RefCell::new(CountMap::new()))); }

        let index = self.add_scope(ConcatScope { consumed: consumed.clone() });

        for id in 0..sources.len() {
            sources[id].connect_to(ScopeInput(index, id as u64),
                                   Counter::new(outputs.clone(), consumed[id].clone()));
        }

        Stream::new(ScopeOutput(index, 0), registrar, self.clone())
    }
}

pub struct ConcatScope<T:Timestamp> {
    consumed:   Vec<Rc<RefCell<CountMap<T>>>>
}

impl<T:Timestamp> Scope<T> for ConcatScope<T> {
    fn name(&self) -> String { format!("Concat") }
    fn inputs(&self) -> u64 { self.consumed.len() as u64 }
    fn outputs(&self) -> u64 { 1 }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut [CountMap<T>],
                                          messages_consumed: &mut [CountMap<T>],
                                          messages_produced: &mut [CountMap<T>]) -> bool
    {
        for (index, updates) in self.consumed.iter().enumerate() {
            while let Some((key, val)) = updates.borrow_mut().pop() {
                messages_consumed[index].update(&key, val);
                messages_produced[0].update(&key, val);
            }
        }

        return false;   // no reason to keep running on Concat's account
    }

    fn notify_me(&self) -> bool { false }
}
