use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Graph, Scope};
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::*;
use communication::channels::ObserverHelper;
use example::stream::Stream;
use columnar::Columnar;

// TODO : This is the only place that OutputPort::clone is used.

pub trait ConcatExt { fn concat(&mut self, &mut Self) -> Self; }

impl<'a, G: Graph+'a, D: Data> ConcatExt for Stream<'a, G, D> {
    fn concat(&mut self, other: &mut Stream<G, D>) -> Stream<'a, G, D> {
        let (outputs, registrar) = OutputPort::<G::Timestamp, D>::new();
        let consumed = vec![Rc::new(RefCell::new(CountMap::new())),
                            Rc::new(RefCell::new(CountMap::new()))];

        let index = self.graph.borrow_mut().add_scope(ConcatScope { consumed: consumed.clone() });

        self.connect_to(ScopeInput(index, 0), ObserverHelper::new(outputs.clone(), consumed[0].clone()));
        other.connect_to(ScopeInput(index, 1), ObserverHelper::new(outputs, consumed[1].clone()));
        self.clone_with(ScopeOutput(index, 0), registrar)
    }
}

pub trait ConcatVecExt<'a, G: Graph+'a, D: Data> { fn concatenate(&mut self) -> Stream<'a, G, D>; }

impl<'a, G: Graph+'a, D: Data> ConcatVecExt<'a, G, D> for Vec<Stream<'a, G, D>> {
    fn concatenate(&mut self) -> Stream<'a, G, D> {
        if self.len() == 0 { panic!("must pass at least one stream to concat"); }

        let (target, registrar) = OutputPort::<G::Timestamp, D>::new();
        let mut consumed = Vec::new();
        for _ in 0..self.len() { consumed.push(Rc::new(RefCell::new(CountMap::new()))); }

        let index = self[0].graph.borrow_mut().add_scope(ConcatScope { consumed: consumed.clone() });

        for id in 0..self.len() {
            self[id].connect_to(ScopeInput(index, id as u64), ObserverHelper::new(target.clone(), consumed[id].clone()));
        }

        self[0].clone_with(ScopeOutput(index, 0), registrar)
    }
}

pub struct ConcatScope<T:Timestamp> {
    consumed:   Vec<Rc<RefCell<CountMap<T>>>>
}

impl<T:Timestamp> Scope<T> for ConcatScope<T> where <T as Columnar>::Stack: 'static {
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
