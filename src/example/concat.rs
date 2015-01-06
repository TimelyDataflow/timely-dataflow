use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::channels::{Data, OutputPort};
use communication::Observer;
use example::stream::Stream;

pub trait ConcatExtensionTrait { fn concat(&mut self, &mut Self) -> Self; }

impl<T, S, D> ConcatExtensionTrait for Stream<T, S, D>
where T:Timestamp,
      S:PathSummary<T>,
      D:Data,
{
    fn concat(&mut self, other: &mut Stream<T, S, D>) -> Stream<T, S, D>
    {
        let messages = vec![Rc::new(RefCell::new(Vec::new())), Rc::new(RefCell::new(Vec::new()))];
        let targets = OutputPort::new();

        let concat = ConcatScope
        {
            consumed:   messages.clone(),
            produced:   targets.updates.clone(),
        };

        let index = self.graph.add_scope(box concat);

        self.graph.connect(self.name, ScopeInput(index, 0));
        other.graph.connect(other.name, ScopeInput(index, 1));

        self.port.borrow_mut().push(box()(messages[0].clone(), targets.clone()));
        other.port.borrow_mut().push(box()(messages[1].clone(), targets.clone()));

        return self.copy_with(ScopeOutput(index, 0), targets.targets.clone());
    }
}

pub struct ConcatScope<T:Timestamp>
{
    consumed:   Vec<Rc<RefCell<Vec<(T, i64)>>>>,       // messages consumed since last asked
    produced:   Rc<RefCell<Vec<(T, i64)>>>,
}

impl<T:Timestamp, S:PathSummary<T>> Scope<T, S> for ConcatScope<T>
{
    fn name(&self) -> String { format!("Concat") }
    fn inputs(&self) -> uint { self.consumed.len() }
    fn outputs(&self) -> uint { 1 }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for (index, updates) in self.consumed.iter().enumerate() {
            for &(key, val) in updates.borrow().iter() {
                messages_consumed[index].update(key, val);
            }

            updates.borrow_mut().clear();
        }

        for &(key, val) in self.produced.borrow().iter() {
            messages_produced[0].update(key, val);
        }

        self.produced.borrow_mut().clear();
        return false;
    }

    fn notify_me(&self) -> bool { false }
}

impl<T:Timestamp, D:Data> Observer<(T, Vec<D>)> for (Rc<RefCell<Vec<(T, i64)>>>, OutputPort<T, D>)
{
    fn next(&mut self, (time, data): (T, Vec<D>))
    {
        self.0.borrow_mut().push((time, data.len() as i64));
        self.1.next((time, data));
    }

    fn done(&mut self) -> () { self.1.done(); }
}
