use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::subgraph::Source::{GraphInput, ScopeOutput};
use progress::subgraph::Target::{GraphOutput, ScopeInput};
use progress::subgraph::{Subgraph, Summary};

use progress::broadcast::ProgressBroadcaster;

use example::stream::Stream;
use communication::Observer;
use communication::channels::{Data, OutputPort};

pub trait GraphBoundary<T1:Timestamp, T2:Timestamp, S1:PathSummary<T1>, S2:PathSummary<T2>> :
{
    // adds an input to self, from source, contained in graph.
    fn add_input<D:Data>(&mut self, source: &mut Stream<T1, S1, D>) ->
        Stream<(T1, T2), Summary<S1, S2>, D>;
    fn add_output_to_graph<D:Data>(&mut self, source: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                              graph: Box<Graph<T1, S1>>) ->
        Stream<T1, S1, D>;


    fn new_subgraph<T, S, B>(&mut self, default: T, broadcaster: B) -> Rc<RefCell<Subgraph<(T1, T2), Summary<S1, S2>, T, S, B>>>
    where T: Timestamp,
          S: PathSummary<T>,
          B: ProgressBroadcaster<((T1, T2), T)>;
}

impl<TOuter, SOuter, TInner, SInner, Bcast>
GraphBoundary<TOuter, TInner, SOuter, SInner>
for Rc<RefCell<Subgraph<TOuter, SOuter, TInner, SInner, Bcast>>>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
      Bcast:  ProgressBroadcaster<(TOuter, TInner)>
{
    fn add_input<D: Data>(&mut self, source: &mut Stream<TOuter, SOuter, D>) ->
        Stream<(TOuter, TInner), Summary<SOuter, SInner>, D>
    {
        let ingress = IngressNub { targets: OutputPort::new(), };

        let listeners = ingress.targets.targets.clone();

        let mut borrow = self.borrow_mut();

        let index = borrow.new_input(ingress.targets.updates.clone());

        source.graph.connect(source.name, ScopeInput(borrow.index, index));
        source.port.borrow_mut().push(box ingress);

        return Stream { name: GraphInput(index), port: listeners, graph: self.as_box(), allocator: source.allocator.clone() };
    }

    fn add_output_to_graph<D: Data>(&mut self, source: &mut Stream<(TOuter, TInner), Summary<SOuter, SInner>, D>,
                                                        graph: Box<Graph<TOuter, SOuter>>) -> Stream<TOuter, SOuter, D>
    {
        let mut borrow = self.borrow_mut();
        let index = borrow.new_output();

        let targets = Rc::new(RefCell::new(Vec::new()));

        borrow.connect(source.name, GraphOutput(index));
        source.port.borrow_mut().push(box EgressNub { targets: targets.clone() });

        return Stream {
            name: ScopeOutput(borrow.index, index),
            port: targets, graph: graph.as_box(),
            allocator: source.allocator.clone() };
    }

    fn new_subgraph<T, S, B>(&mut self, _default: T, broadcaster: B) -> Rc<RefCell<Subgraph<(TOuter, TInner), Summary<SOuter, SInner>, T, S, B>>>
    where T: Timestamp,
          S: PathSummary<T>,
          B: ProgressBroadcaster<((TOuter, TInner), T)>
    {
        let mut result: Subgraph<(TOuter, TInner), Summary<SOuter, SInner>, T, S, B> = Default::default();
        result.index = self.borrow().subscopes.len();
        result.broadcaster = broadcaster;
        return Rc::new(RefCell::new(result));
    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: OutputPort<(TOuter, TInner), TData>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer<(TOuter, Vec<TData>)> for IngressNub<TOuter, TInner, TData>
{
    fn next(&mut self, (time, data): (TOuter, Vec<TData>)) {
        self.targets.next(((time, Default::default()), data));
    }
    fn done(&mut self) -> () { self.targets.done(); }
}


pub struct EgressNub<TOuter, TInner, TData> {
    targets: Rc<RefCell<Vec<Box<Observer<(TOuter, Vec<TData>)>>>>>,
}

impl<TOuter, TInner, TData> Observer<((TOuter, TInner), Vec<TData>)> for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    fn next(&mut self, (time, data): ((TOuter, TInner), Vec<TData>)) {
        for target in self.targets.borrow_mut().iter_mut() {
            target.next((time.0, data.clone()));
        }
    }
    fn done(&mut self) -> () {
        for target in self.targets.borrow_mut().iter_mut() { target.done(); }
    }
}
