use std::hash::Hash;
use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;

use progress::{Timestamp, CountMap};
use progress::nested::subgraph::Source::{GraphInput, ChildOutput};
use progress::nested::subgraph::Target::{GraphOutput, ChildInput};
use progress::nested::product::Product;
use {Data, Push};
use dataflow::channels::pushers::{Counter, Tee};
use dataflow::channels::Content;

use dataflow::{Stream, Scope};
use dataflow::scopes::Child;
use dataflow::operators::delay::*;

pub trait Enter<G: Scope, T: Timestamp, D: Data> {
    fn enter(&self, &Stream<G, D>) -> Stream<Child<G, T>, D>;
}

pub trait EnterAt<G: Scope, T: Timestamp, D: Data>  where  G::Timestamp: Hash, T: Hash {
    fn enter_at<F:Fn(&D)->T+'static>(&self, stream: &Stream<G, D>, initial: F) -> Stream<Child<G, T>, D> ;
}

impl<G: Scope, T: Timestamp, D: Data, E: Enter<G, T, D>> EnterAt<G, T, D> for E
where G::Timestamp: Hash, T: Hash {
    fn enter_at<F:Fn(&D)->T+'static>(&self, stream: &Stream<G, D>, initial: F) ->
        Stream<Child<G, T>, D> {
            self.enter(stream).delay(move |datum, time| Product::new(time.outer, initial(datum)))
    }
}

impl<T: Timestamp, G: Scope, D: Data>
Enter<G, T, D> for Child<G, T> {
    fn enter(&self, stream: &Stream<G, D>) -> Stream<Child<G, T>, D> {

        let (targets, registrar) = Tee::<Product<G::Timestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: Counter::new(targets, produced.clone()) };

        let scope_index = self.subgraph.borrow().index;
        let input_index = self.subgraph.borrow_mut().new_input(produced);

        stream.connect_to(ChildInput(scope_index, input_index), ingress);

        Stream::new(GraphInput(input_index), registrar, self.clone())
    }
}

pub trait Leave<G: Scope, D: Data> {
    fn leave(&self) -> Stream<G, D>;
}

impl<G: Scope, D: Data, T: Timestamp> Leave<G, D> for Stream<Child<G, T>, D> {
    fn leave(&self) -> Stream<G, D> {

        let scope = self.scope();

        let output_index = scope.subgraph.borrow_mut().new_output();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        self.connect_to(GraphOutput(output_index), EgressNub { targets: targets, phantom: PhantomData });
        let subgraph_index = scope.subgraph.borrow().index;
        Stream::new(ChildOutput(subgraph_index, output_index),
                    registrar,
                    scope.parent.clone())
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
