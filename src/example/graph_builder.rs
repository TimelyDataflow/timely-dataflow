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

pub trait EnterSubgraphExt<'aa, 'bb: 'aa, GOuter: Graph+'bb, TInner: Timestamp, D> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut SubgraphBuilder<'aa, 'bb, GOuter, TInner>>) -> Stream<'a, 'b, SubgraphBuilder<'aa, 'bb, GOuter, TInner>, D> where 'b: 'a, 'aa: 'b, 'bb: 'aa;
}

impl<'aa, 'bb: 'aa, GOuter: Graph+'bb, TInner: Timestamp, D: Data> EnterSubgraphExt<'aa, 'bb, GOuter, TInner, D>
for Stream<'aa, 'bb, GOuter, D> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut SubgraphBuilder<'aa, 'bb, GOuter, TInner>>) -> Stream<'a, 'b, SubgraphBuilder<'aa, 'bb, GOuter, TInner>, D> where 'b: 'a, 'aa: 'b, 'bb: 'aa {

        let targets = OutputPort::<Product<GOuter::Timestamp, TInner>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let scope_index = subgraph.borrow().subgraph.index;
        let input_index = subgraph.borrow_mut().subgraph.new_input(produced);

        self.connect_to(ScopeInput(scope_index, input_index), ingress);

        Stream::new(GraphInput(input_index), targets, subgraph)
    }
}

pub trait LeaveSubgraphExt<'a, 'b: 'a, GOuter: Graph+'b, D: Data> {
    fn leave(&mut self) -> Stream<'a, 'b, GOuter, D>;
}

use progress::nested::subgraph::SubgraphBuilder;
impl<'aa, 'bb, 'a, 'b: 'a, GOuter: Graph+'b, TInner: Timestamp, D: Data> LeaveSubgraphExt<'a, 'b, GOuter, D>
for Stream<'aa, 'bb, SubgraphBuilder<'a, 'b, GOuter, TInner>, D> {
    fn leave(&mut self) -> Stream<'a, 'b, GOuter, D> {

        let index = self.graph.borrow_mut().subgraph.new_output();
        let targets = OutputPort::<GOuter::Timestamp, D>::new();

        self.connect_to(GraphOutput(index), EgressNub { targets: targets.clone(), phantom: PhantomData });

        Stream::new(ScopeOutput(self.graph.borrow().subgraph.index, index), targets, self.graph.borrow().parent)
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
