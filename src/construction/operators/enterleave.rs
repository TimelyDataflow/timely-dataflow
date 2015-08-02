use std::hash::Hash;
use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;

use progress::{Timestamp, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ChildOutput};
use progress::nested::subgraph::Target::{GraphOutput, ChildInput};
use progress::nested::product::Product;
use communication::{Data, Message, Observer};
use communication::observer::{Counter, Tee};

use construction::{Stream, GraphBuilder};
use construction::builder::SubgraphBuilder;
use construction::operators::delay::*;

pub trait EnterSubgraphExt<G: GraphBuilder, T: Timestamp, D: Data> {
    fn enter(&self, &Stream<G, D>) -> Stream<SubgraphBuilder<G, T>, D>;
}

pub trait EnterSubgraphAtExt<G: GraphBuilder, T: Timestamp, D: Data>  where  G::Timestamp: Hash, T: Hash {
    fn enter_at<F:Fn(&D)->T+'static>(&self, stream: &Stream<G, D>, initial: F) -> Stream<SubgraphBuilder<G, T>, D> ;
}

impl<G: GraphBuilder, T: Timestamp, D: Data, E: EnterSubgraphExt<G, T, D>> EnterSubgraphAtExt<G, T, D> for E
where G::Timestamp: Hash, T: Hash {
    fn enter_at<F:Fn(&D)->T+'static>(&self, stream: &Stream<G, D>, initial: F) ->
        Stream<SubgraphBuilder<G, T>, D> {
            self.enter(stream).delay(move |datum, time| Product::new(time.outer, initial(datum)))
    }
}

impl<T: Timestamp, G: GraphBuilder, D: Data>
EnterSubgraphExt<G, T, D> for SubgraphBuilder<G, T> {
    fn enter(&self, stream: &Stream<G, D>) -> Stream<SubgraphBuilder<G, T>, D> {

        let (targets, registrar) = Tee::<Product<G::Timestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: Counter::new(targets, produced.clone()) };

        let scope_index = self.subgraph.borrow().index;
        let input_index = self.subgraph.borrow_mut().new_input(produced);

        stream.connect_to(ChildInput(scope_index, input_index), ingress);

        Stream::new(GraphInput(input_index), registrar, self.clone())
    }
}

pub trait LeaveSubgraphExt<G: GraphBuilder, D: Data> {
    fn leave(&self) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data, T: Timestamp> LeaveSubgraphExt<G, D> for Stream<SubgraphBuilder<G, T>, D> {
    fn leave(&self) -> Stream<G, D> {

        let builder = self.builder();

        let output_index = builder.subgraph.borrow_mut().new_output();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        self.connect_to(GraphOutput(output_index), EgressNub { targets: targets, phantom: PhantomData });
        let subgraph_index = builder.subgraph.borrow().index;
        Stream::new(ChildOutput(subgraph_index, output_index),
                    registrar,
                    builder.parent.clone())
    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Counter<Tee<Product<TOuter, TInner>, TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer for IngressNub<TOuter, TInner, TData> {
    type Time = TOuter;
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &TOuter) -> () { self.targets.open(&Product::new(time.clone(), Default::default())); }
    #[inline(always)] fn shut(&mut self, time: &TOuter) -> () { self.targets.shut(&Product::new(time.clone(), Default::default())); }
    #[inline(always)] fn give(&mut self, data: &mut Message<TData>) { self.targets.give(data); }
}


pub struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Tee<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Observer for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    type Time = Product<TOuter, TInner>;
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &Product<TOuter, TInner>) { self.targets.open(&time.outer); }
    #[inline(always)] fn shut(&mut self, time: &Product<TOuter, TInner>) { self.targets.shut(&time.outer); }
    #[inline(always)] fn give(&mut self, data: &mut Message<TData>) { self.targets.give(data); }
}
