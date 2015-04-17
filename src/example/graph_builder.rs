use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::{Timestamp, Graph, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ScopeOutput};
use progress::nested::subgraph::Target::{GraphOutput, ScopeInput};
// use progress::nested::subgraph::Subgraph;
use progress::nested::product::Product;
use progress::nested::builder::Builder as SubgraphBuilder;

use example::stream::Stream;
use communication::{Observer, Communicator};
use communication::channels::{Data, OutputPort, ObserverHelper};

// TODO : Make this trait implemented only by SubgraphBuilder, so that outer streams
// TODO : are not tempted to try and use a possible busy RefCell<&mut GOuter>.
pub trait EnterSubgraphExt<'aa, GOuter: Graph+'aa, TInner: Timestamp, D> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut SubgraphBuilder<'aa, GOuter, TInner>>) ->
        Stream<'a, &'b mut SubgraphBuilder<'aa, GOuter, TInner>, D> where 'b: 'a, 'aa: 'b;
}

impl<'aa, GOuter: Graph+'aa, TInner: Timestamp, D: Data> EnterSubgraphExt<'aa, GOuter, TInner, D>
for Stream<'aa, GOuter, D> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut SubgraphBuilder<'aa, GOuter, TInner>>) ->
        Stream<'a, &'b mut SubgraphBuilder<'aa, GOuter, TInner>, D> where 'b: 'a, 'aa: 'b {

        let targets = OutputPort::<Product<GOuter::Timestamp, TInner>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let scope_index = subgraph.borrow().index();
        let input_index = subgraph.borrow_mut().new_input(produced);

        // NOTE : Risky to assume outer graph available in inner graph;
        // NOTE : better to use inner graph's reference to outer graph.
        // self.connect_to(ScopeInput(scope_index, input_index), ingress);
        subgraph.borrow_mut().parent().borrow_mut().connect(self.name, ScopeInput(scope_index, input_index));
        self.ports.add_observer(ingress);

        Stream::new(GraphInput(input_index), targets, subgraph)
    }
}

pub trait LeaveSubgraphExt<'a, GOuter: Graph+'a, D: Data> {
    fn leave(&mut self) -> Stream<'a, GOuter, D>;
}

impl<'aa, 'b: 'aa, 'a: 'b, GOuter: Graph+'a, TInner: Timestamp, D: Data> LeaveSubgraphExt<'a, GOuter, D>
for Stream<'aa, &'b mut SubgraphBuilder<'a, GOuter, TInner>, D>
where GOuter::Communicator : 'b {
    fn leave(&mut self) -> Stream<'a, GOuter, D> {

        let output_index = self.graph.borrow_mut().new_output();
        let targets = OutputPort::<GOuter::Timestamp, D>::new();

        self.connect_to(GraphOutput(output_index), EgressNub { targets: targets.clone(), phantom: PhantomData });

        Stream::new(ScopeOutput(self.graph.borrow().index(), output_index), targets, self.graph.borrow().parent())
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
