use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::graph::GraphExtension;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::channels::{Data, OutputPort, ObserverHelper};
use communication::Observer;
use example::stream::Stream;

pub trait ConcatExtensionTrait { fn concat(&mut self, &mut Self) -> Self; }

impl<T, S, D> ConcatExtensionTrait for Stream<T, S, D>
where T:Timestamp,
      S:PathSummary<T>,
      D:Data,
{
    fn concat(&mut self, other: &mut Stream<T, S, D>) -> Stream<T, S, D> {
        let targets: Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>> = Rc::new(RefCell::new(Vec::new()));
        let consumed = vec![Rc::new(RefCell::new(Vec::new())),
                            Rc::new(RefCell::new(Vec::new()))];

        let index = self.graph.add_scope(ConcatScope { consumed: consumed.clone() });

        self.graph.connect(self.name, ScopeInput(index, 0));
        other.graph.connect(other.name, ScopeInput(index, 1));

        self.add_observer(ObserverHelper::new(OutputPort { shared: targets.clone() }, consumed[0].clone()));
        other.add_observer(ObserverHelper::new(OutputPort { shared: targets.clone() }, consumed[1].clone()));

        Stream {
            name: ScopeOutput(index, 0),
            ports: targets,
            graph: self.graph.as_box(),
            allocator: self.allocator.clone(),
        }
    }
}

pub struct ConcatScope<T:Timestamp> {
    consumed:   Vec<Rc<RefCell<Vec<(T, i64)>>>>
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for ConcatScope<T> {
    fn name(&self) -> String { format!("Concat") }
    fn inputs(&self) -> u64 { self.consumed.len() as u64 }
    fn outputs(&self) -> u64 { 1 }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for (index, updates) in self.consumed.iter().enumerate() {
            for (key, val) in updates.borrow_mut().drain() {
                messages_consumed[index].update(&key, val);
                messages_produced[0].update(&key, val);
            }
        }

        return false;   // no reason to keep running on Concat's account
    }

    fn notify_me(&self) -> bool { false }
}
