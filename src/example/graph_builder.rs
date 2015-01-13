use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::subgraph::Source::{GraphInput, ScopeOutput};
use progress::subgraph::Target::{GraphOutput, ScopeInput};
use progress::subgraph::{Subgraph, Summary};

use progress::broadcast::Progcaster;

use example::stream::Stream;
use communication::Observer;
use communication::channels::{Data, OutputPort, ObserverHelper};

pub trait GraphBoundary<T1:Timestamp, T2:Timestamp, S1:PathSummary<T1>, S2:PathSummary<T2>> :
{
    // adds an input to self, from source, contained in graph.
    fn add_input<D:Data>(&mut self, source: &mut Stream<T1, S1, D>) -> Stream<(T1, T2), Summary<S1, S2>, D>;
    fn add_output_to_graph<D:Data>(&mut self, source: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                              graph: Box<Graph<T1, S1>>) -> Stream<T1, S1, D>;

    fn new_subgraph<T, S>(&mut self, default: T, progcaster: Progcaster<((T1,T2),T)>) -> Rc<RefCell<Subgraph<(T1, T2), Summary<S1, S2>, T, S>>>
    where T: Timestamp,
          S: PathSummary<T>;
}

impl<TOuter, SOuter, TInner, SInner>
GraphBoundary<TOuter, TInner, SOuter, SInner>
for Rc<RefCell<Subgraph<TOuter, SOuter, TInner, SInner>>>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
{
    fn add_input<D: Data>(&mut self, source: &mut Stream<TOuter, SOuter, D>) ->
        Stream<(TOuter, TInner), Summary<SOuter, SInner>, D>
    {
        let targets = Rc::new(RefCell::new(Vec::new()));
        let produced = Rc::new(RefCell::new(Vec::new()));

        let ingress = IngressNub { targets: ObserverHelper::new(OutputPort{ shared: targets.clone() }, produced.clone()) };

        let mut borrow = self.borrow_mut();
        let index = borrow.new_input(produced);

        source.graph.connect(source.name, ScopeInput(borrow.index, index));
        source.add_observer(ingress);

        return Stream { name: GraphInput(index), ports: targets, graph: self.as_box(), allocator: source.allocator.clone() };
    }

    fn add_output_to_graph<D: Data>(&mut self, source: &mut Stream<(TOuter, TInner), Summary<SOuter, SInner>, D>,
                                               graph: Box<Graph<TOuter, SOuter>>) -> Stream<TOuter, SOuter, D>
    {
        let mut borrow = self.borrow_mut();
        let index = borrow.new_output();

        let targets = Rc::new(RefCell::new(Vec::new()));

        borrow.connect(source.name, GraphOutput(index));
        source.add_observer(EgressNub { targets: targets.clone() });

        return Stream {
            name: ScopeOutput(borrow.index, index),
            ports: targets,
            graph: graph.as_box(),
            allocator: source.allocator.clone() };
    }

    fn new_subgraph<T, S>(&mut self, _default: T, progcaster: Progcaster<((TOuter,TInner),T)>)
            -> Rc<RefCell<Subgraph<(TOuter, TInner), Summary<SOuter, SInner>, T, S>>>
    where T: Timestamp,
          S: PathSummary<T>,
    {
        let mut result: Subgraph<(TOuter, TInner), Summary<SOuter, SInner>, T, S> = Default::default();
        result.index = self.borrow().children() as u64;
        result.progcaster = progcaster;
        return Rc::new(RefCell::new(result));
    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: ObserverHelper<OutputPort<(TOuter, TInner), TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer for IngressNub<TOuter, TInner, TData>
{
    type Time = TOuter;
    type Data = TData;
    fn push(&mut self, data: &TData) { self.targets.push(data); }
    fn open(&mut self, time: &TOuter) -> () { self.targets.open(&(time.clone(), Default::default())); }
    fn shut(&mut self, time: &TOuter) -> () { self.targets.shut(&(time.clone(), Default::default())); }
}


pub struct EgressNub<TOuter, TInner, TData> {
    targets: Rc<RefCell<Vec<Box<Observer<Time=TOuter, Data=TData>>>>>,
}

impl<TOuter, TInner, TData> Observer for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    type Time = (TOuter, TInner);
    type Data = TData;
    fn open(&mut self, time: &(TOuter, TInner)) { for target in self.targets.borrow_mut().iter_mut() { target.open(&time.0); } }
    fn push(&mut self, data: &TData) { for target in self.targets.borrow_mut().iter_mut() { target.push(data); } }
    fn shut(&mut self, time: &(TOuter, TInner)) { for target in self.targets.borrow_mut().iter_mut() { target.shut(&time.0); } }
}
