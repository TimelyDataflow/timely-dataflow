use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Graph, Scope};
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::channels::{Data, OutputPort, ObserverHelper};
use example::stream::Stream;
use columnar::Columnar;

pub trait ConcatExtensionTrait { fn concat(&mut self, &mut Self) -> Self; }

impl<G: Graph, D: Data> ConcatExtensionTrait for Stream<G, D> {
    fn concat(&mut self, other: &mut Stream<G, D>) -> Stream<G, D> {
        let outputs = OutputPort::<G::Timestamp, D>::new();
        let consumed = vec![Rc::new(RefCell::new(CountMap::new())),
                            Rc::new(RefCell::new(CountMap::new()))];

        let index = self.graph.add_scope(ConcatScope { consumed: consumed.clone() });

        self.connect_to(ScopeInput(index, 0), ObserverHelper::new(outputs.clone(), consumed[0].clone()));
        other.connect_to(ScopeInput(index, 1), ObserverHelper::new(outputs.clone(), consumed[1].clone()));
        self.clone_with(ScopeOutput(index, 0), outputs)
    }
}

pub struct ConcatScope<T:Timestamp> {
    consumed:   Vec<Rc<RefCell<CountMap<T>>>>
}

impl<T:Timestamp> Scope<T> for ConcatScope<T> where <T as Columnar>::Stack: 'static {
    fn name(&self) -> String { format!("Concat") }
    fn inputs(&self) -> u64 { self.consumed.len() as u64 }
    fn outputs(&self) -> u64 { 1 }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<CountMap<T>>,
                                          messages_consumed: &mut Vec<CountMap<T>>,
                                          messages_produced: &mut Vec<CountMap<T>>) -> bool
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
