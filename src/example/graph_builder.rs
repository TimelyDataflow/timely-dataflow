use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::{Timestamp, Graph, Scope, CountMap};
use progress::subgraph::Source::{GraphInput, ScopeOutput};
use progress::subgraph::Target::{GraphOutput, ScopeInput};
use progress::subgraph::Subgraph;

use example::stream::Stream;
use communication::Observer;
use communication::channels::{Data, OutputPort, ObserverHelper};

pub trait GraphBoundary<G1: Graph, G2: Graph, T2: Timestamp>
where G2 : Graph<Timestamp = (G1::Timestamp, T2)>
{
    // adds an input to self, from source, contained in graph.
    fn add_input<D:Data>(&mut self, source: &mut Stream<G1, D>) -> Stream<G2, D>;
    fn add_output_to_graph<D:Data>(&mut self, source: &mut Stream<G2, D>, graph: &G1) -> Stream<G1, D>;

}


impl<GOuter: Graph, TInner: Timestamp>
GraphBoundary<GOuter, Self, TInner>
for Rc<RefCell<Subgraph<GOuter::Timestamp, TInner>>>
where GOuter : Graph,
      TInner: Timestamp,
{
    fn add_input<D: Data>(&mut self, source: &mut Stream<GOuter, D>) -> Stream<Self, D> {
        let targets: OutputPort<(GOuter::Timestamp, TInner), D> = Default::default();
        let produced = Rc::new(RefCell::new(CountMap::new()));

        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let mut borrow = self.borrow_mut();
        let index = borrow.new_input(produced);

        source.graph.connect(source.name, ScopeInput(borrow.index, index));
        source.add_observer(ingress);

        Stream {
            name: GraphInput(index),
            ports: targets,
            graph: self.clone(),
            allocator: source.allocator.clone()
        }
    }

    fn add_output_to_graph<D: Data>(&mut self, source: &mut Stream<Self, D>, graph: &GOuter) -> Stream<GOuter, D> {
        let mut borrow = self.borrow_mut();
        let index = borrow.new_output();

        let targets: OutputPort<GOuter::Timestamp, D> = Default::default();

        borrow.connect(source.name, GraphOutput(index));
        source.add_observer(EgressNub { targets: targets.clone(), phantom: PhantomData });

        Stream {
            name: ScopeOutput(borrow.index, index),
            ports: targets,
            graph: graph.clone(),
            allocator: source.allocator.clone()
        }
    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: ObserverHelper<OutputPort<(TOuter, TInner), TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer for IngressNub<TOuter, TInner, TData> {
    type Time = TOuter;
    type Data = TData;
    #[inline(always)] fn push(&mut self, data: &TData) { self.targets.push(data); }
    #[inline(always)] fn open(&mut self, time: &TOuter) -> () { self.targets.open(&(time.clone(), Default::default())); }
    #[inline(always)] fn shut(&mut self, time: &TOuter) -> () { self.targets.shut(&(time.clone(), Default::default())); }
}


pub struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: OutputPort<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Observer for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    type Time = (TOuter, TInner);
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &(TOuter, TInner)) { self.targets.open(&time.0); }
    #[inline(always)] fn push(&mut self, data: &TData) { self.targets.push(data); }
    #[inline(always)] fn shut(&mut self, time: &(TOuter, TInner)) { self.targets.shut(&time.0); }
}
