use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::{Timestamp, Graph, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ScopeOutput};
use progress::nested::subgraph::Target::{GraphOutput, ScopeInput};
use progress::nested::product::Product;
use communication::{Observer, Communicator};
use communication::channels::{Data, OutputPort, ObserverHelper};

use example_static::builder::{GraphBuilder, SubgraphBuilder};
use example_static::stream::{Stream, ActiveStream};

pub trait EnterSubgraphExt<'outer, G: GraphBuilder+'outer, T: Timestamp, D: Data> {
    fn enter<'inner>(&'inner mut self, &mut Stream<G::Timestamp, D>) ->
        ActiveStream<'inner, SubgraphBuilder<'outer, G, T>, D> where 'outer: 'inner;
}

impl<'outer, T: Timestamp, G: GraphBuilder+'outer, D: Data>
EnterSubgraphExt<'outer, G, T, D> for SubgraphBuilder<'outer, G, T> {
    fn enter<'inner>(&'inner mut self, stream: &mut Stream<G::Timestamp, D>) ->
        ActiveStream<'inner, SubgraphBuilder<'outer, G, T>, D> where 'outer: 'inner {

        let targets = OutputPort::<Product<G::Timestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let scope_index = self.subgraph.index;
        let input_index = self.subgraph.new_input(produced);

        self.parent.connect(stream.name, ScopeInput(scope_index, input_index));
        stream.ports.add_observer(ingress);

        Stream::new(GraphInput(input_index), targets).enable(self)
    }
}

pub trait LeaveSubgraphExt<'outer, G: GraphBuilder+'outer, D: Data> {
    fn leave(&mut self) -> Stream<G::Timestamp, D>;
}

impl<'inner, 'outer: 'inner, G: GraphBuilder+'outer, D: Data, TInner: Timestamp> LeaveSubgraphExt<'outer, G, D>
for ActiveStream<'inner, SubgraphBuilder<'outer, G, TInner>, D> {
    fn leave(&mut self) -> Stream<G::Timestamp, D> {

        let output_index = self.builder.subgraph.new_output();
        let targets = OutputPort::<G::Timestamp, D>::new();
        self.connect_to(GraphOutput(output_index), EgressNub { targets: targets.clone(), phantom: PhantomData });
        Stream::new(ScopeOutput(self.builder.subgraph.index, output_index), targets)
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
