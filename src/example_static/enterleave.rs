use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;

use progress::{Timestamp, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ScopeOutput};
use progress::nested::subgraph::Target::{GraphOutput, ScopeInput};
use progress::nested::product::Product;
use communication::*;
use communication::channels::ObserverHelper;

use example_static::builder::{GraphBuilder, SubgraphBuilder};
use example_static::stream::*;
use example_static::delay::*;

pub trait EnterSubgraphExt<G: GraphBuilder, T: Timestamp, D: Data> {
    fn enter<'a>(&'a mut self, &Stream<G::Timestamp, D>) ->
        ActiveStream<&'a mut SubgraphBuilder<G, T>, D>;
    fn enter_at<'a, F:Fn(&D)->T+'static>(&'a mut self, stream: &Stream<G::Timestamp, D>, initial: F) ->
        ActiveStream<&'a mut SubgraphBuilder<G, T>, D> where  G::Communicator: 'a {
            self.enter(stream).delay(move |datum, time| Product::new(time.outer, initial(datum)))
        }
}

impl<T: Timestamp, G: GraphBuilder, D: Data>
EnterSubgraphExt<G, T, D> for SubgraphBuilder<G, T> {
    fn enter<'a>(&'a mut self, stream: &Stream<G::Timestamp, D>) ->
        ActiveStream<&'a mut SubgraphBuilder<G, T>, D> {

        let (targets, registrar) = OutputPort::<Product<G::Timestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets, produced.clone()) };

        let scope_index = self.subgraph.index;
        let input_index = self.subgraph.new_input(produced);

        self.parent.connect(stream.name, ScopeInput(scope_index, input_index));
        stream.ports.add_observer(ingress);

        self.enable(&Stream::new(GraphInput(input_index), registrar))
    }
}

pub trait LeaveSubgraphExt<G: GraphBuilder, D: Data> {
    fn leave(&mut self) -> Stream<G::Timestamp, D>;
}

impl<'a, G: GraphBuilder+'a, D: Data, TInner: Timestamp> LeaveSubgraphExt<G, D>
for ActiveStream<&'a mut SubgraphBuilder<G, TInner>, D> where G::Communicator: 'a {
    fn leave(&mut self) -> Stream<G::Timestamp, D> {

        let output_index = self.builder.subgraph.new_output();
        let (targets, registrar) = OutputPort::<G::Timestamp, D>::new();
        self.connect_to(GraphOutput(output_index), EgressNub { targets: targets, phantom: PhantomData });
        Stream::new(ScopeOutput(self.builder.subgraph.index, output_index), registrar)
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
