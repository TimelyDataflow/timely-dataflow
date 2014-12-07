use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use example::ports::{SourcePort, TargetPort};
use example::stream::Stream;

pub trait ConcatExtensionTrait { fn concat(&mut self, &mut Self) -> Self; }

impl<T, S, D> ConcatExtensionTrait for Stream<T, S, D>
where T:Timestamp,
      S:PathSummary<T>,
      D:Copy+'static,
{
    fn concat(&mut self, other: &mut Stream<T, S, D>) -> Stream<T, S, D>
    {
        let concat = Rc::new(RefCell::new(ConcatScope
        {
            messages:   Vec::from_fn(2, |_| Vec::new()),
            targets:    Vec::new(),
        }));

        let index = self.graph.add_scope(box concat.clone());

        self.graph.connect(self.name, ScopeInput(index, 0));
        other.graph.connect(other.name, ScopeInput(index, 1));

        self.port.register_interest(box()(concat.clone(), 0));
        other.port.register_interest(box()(concat.clone(), 1));

        return Stream{ name:ScopeOutput(index, 0), port: box concat, graph: self.graph.as_box() };
    }
}

pub struct ConcatScope<T:Timestamp, D:Copy+'static>
{
    messages:   Vec<Vec<(T, i64)>>,       // messages consumed since last asked
    targets:    Vec<Box<TargetPort<T, D>>>, // places to send things
}

impl<T:Timestamp, S:PathSummary<T>, D:Copy+'static> Scope<T, S> for Rc<RefCell<ConcatScope<T, D>>>
{
    fn name(&self) -> String { format!("Concat") }
    fn inputs(&self) -> uint { self.borrow().messages.len() }
    fn outputs(&self) -> uint { 1 }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> ()
    {
        let mut concat = self.borrow_mut();

        for input in range(0, concat.messages.len())
        {
            for &(key, val) in concat.messages[input].iter()
            {
                messages_consumed[input].push((key, val));
                messages_produced[0].push((key, val));
            }

            concat.messages[input].clear();
        }
    }

    fn notify_me(&self) -> bool { false }
}

impl<T:Timestamp, D:Copy+'static> SourcePort<T, D> for Rc<RefCell<ConcatScope<T, D>>>
{
    fn register_interest(&mut self, target: Box<TargetPort<T, D>>) -> ()
    {
        self.borrow_mut().targets.push(target);
    }
}

impl<T:Timestamp, D:Copy+'static> TargetPort<T, D> for (Rc<RefCell<ConcatScope<T, D>>>, uint)
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>)
    {
        let (ref rc, index) = *self;
        let mut borrow = rc.borrow_mut();

        borrow.messages[index].update(*time, 1);

        for target in borrow.targets.iter_mut()
        {
            target.deliver_data(time, data);
        }
    }
}
