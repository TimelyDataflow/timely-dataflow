use std::hash::Hash;
use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;

use progress::{Timestamp, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ChildOutput};
use progress::nested::subgraph::Target::{GraphOutput, ChildInput};
use progress::nested::product::Product;
// use communication::{Data, Message, Observer};
use fabric::{Push};
use ::Data;
use communication::observer::{Counter, Tee};
use communication::message::Content;

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
    targets: Counter<Product<TOuter, TInner>, TData, Tee<Product<TOuter, TInner>, TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Push<(TOuter, Content<TData>)> for IngressNub<TOuter, TInner, TData> {
    fn push(&mut self, message: &mut Option<(TOuter, Content<TData>)>) {
        if let Some((ref time, ref mut data)) = *message {
            let content = ::std::mem::replace(data, Content::Typed(Vec::new()));
            let mut message = Some((Product::new(time.clone(), Default::default()), content));
            self.targets.push(&mut message);
            if let Some((_, content)) = message {
                *data = content;
            }
        }
        else { self.targets.done(); }
    }
}


pub struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Tee<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Push<(Product<TOuter, TInner>, Content<TData>)> for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    fn push(&mut self, message: &mut Option<(Product<TOuter, TInner>, Content<TData>)>) {
        if let Some((ref time, ref mut data)) = *message {
            let content = ::std::mem::replace(data, Content::Typed(Vec::new()));
            let mut message = Some((time.outer.clone(), content));
            self.targets.push(&mut message);
            if let Some((_, content)) = message {
                *data = content;
            }
        }
        else { self.targets.done(); }
    }
}
