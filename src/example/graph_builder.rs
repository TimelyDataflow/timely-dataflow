use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::{Timestamp, Graph, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ScopeOutput};
use progress::nested::subgraph::Target::{GraphOutput, ScopeInput};
use progress::nested::subgraph::Subgraph;
use progress::nested::product::Product;

use example::stream::Stream;
use communication::{Observer, Communicator};
use communication::channels::{Data, OutputPort, ObserverHelper};

// pub trait EnterSubgraphExt<TOuter: Timestamp, TInner: Timestamp, D: Data, C: Communicator> {
//     fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut Subgraph<TOuter, TInner>>, communicator: C) -> Stream<(&'a RefCell<&'b mut Subgraph<TOuter, TInner>>, C), D>;
// }
//
// impl<GOuter: Graph, TInner: Timestamp, D: Data, C: Communicator + Clone> EnterSubgraphExt<GOuter::Timestamp, TInner, D, C> for Stream<GOuter, D> {
//     fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut Subgraph<GOuter::Timestamp, TInner>>, communicator: C) -> Stream<(&'a RefCell<&'b mut Subgraph<GOuter::Timestamp, TInner>>, C), D> {

pub trait EnterSubgraphExt<TOuter: Timestamp, TInner: Timestamp, D: Data, C: Communicator> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut (Subgraph<TOuter, TInner>, C)>) -> Stream<'a, 'b, (Subgraph<TOuter, TInner>, C), D>;
}

impl<'aa, 'bb, GOuter: Graph, TInner: Timestamp, D: Data, C: Communicator> EnterSubgraphExt<GOuter::Timestamp, TInner, D, C> for Stream<'aa, 'bb, GOuter, D> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut (Subgraph<GOuter::Timestamp, TInner>, C)>) -> Stream<'a, 'b, (Subgraph<GOuter::Timestamp, TInner>, C), D> {

        let targets = OutputPort::<Product<GOuter::Timestamp, TInner>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let scope_index = subgraph.borrow().0.index;
        let input_index = subgraph.borrow_mut().0.new_input(produced);

        self.connect_to(ScopeInput(scope_index, input_index), ingress);

        Stream::new(GraphInput(input_index), targets, subgraph)
    }
}

// pub trait LeaveSubgraphExt<GOuter: Graph, D: Data> {
//     fn leave(&mut self, graph: &GOuter) -> Stream<GOuter, D>;
// }
//
// impl<'a, 'b, GOuter: Graph, TInner: Timestamp, D: Data> LeaveSubgraphExt<GOuter, D> for Stream<(&'a RefCell<&'b mut Subgraph<GOuter::Timestamp, TInner>>, GOuter::Communicator), D> {
//     fn leave(&mut self, graph: &GOuter) -> Stream<GOuter, D> {

pub trait LeaveSubgraphExt<GOuter: Graph, D: Data> {
    fn leave<'a, 'b>(&mut self, graph: &'a RefCell<&'b mut GOuter>) -> Stream<'a, 'b, GOuter, D>  where 'b: 'a, GOuter: 'b ;
}

impl<'aa, 'bb, 'comm,  GOuter: Graph, TInner: Timestamp, D: Data> LeaveSubgraphExt<GOuter, D> for Stream<'aa, 'bb, (Subgraph<GOuter::Timestamp, TInner>, &'comm mut GOuter::Communicator), D> where GOuter::Communicator: 'comm {
    fn leave<'a, 'b>(&mut self, graph: &'a RefCell<&'b mut GOuter>) -> Stream<'a, 'b, GOuter, D>  where 'b: 'a, GOuter: 'b  {

        let index = self.graph.borrow_mut().0.new_output();
        let targets = OutputPort::<GOuter::Timestamp, D>::new();

        self.connect_to(GraphOutput(index), EgressNub { targets: targets.clone(), phantom: PhantomData });

        Stream::new(ScopeOutput(self.graph.borrow_mut().0.index, index), targets, graph)
    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: ObserverHelper<OutputPort<Product<TOuter, TInner>, TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer for IngressNub<TOuter, TInner, TData> {
    type Time = TOuter;
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &TOuter) -> () { self.targets.open(&Product::new(time.clone(), Default::default())); }
    #[inline(always)] fn show(&mut self, data: &TData) { self.targets.show(data); }
    #[inline(always)] fn give(&mut self, data:  TData) { self.targets.give(data); }
    #[inline(always)] fn shut(&mut self, time: &TOuter) -> () { self.targets.shut(&Product::new(time.clone(), Default::default())); }
}


pub struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: OutputPort<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Observer for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    type Time = Product<TOuter, TInner>;
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &Product<TOuter, TInner>) { self.targets.open(&time.outer); }
    #[inline(always)] fn show(&mut self, data: &TData) { self.targets.show(data); }
    #[inline(always)] fn give(&mut self, data:  TData) { self.targets.give(data); }
    #[inline(always)] fn shut(&mut self, time: &Product<TOuter, TInner>) { self.targets.shut(&time.outer); }
}
