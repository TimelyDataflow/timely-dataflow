use std::mem;

use progress::{Timestamp, Scope, Subgraph};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use progress::nested::scope_wrapper::ScopeWrapper;
use communication::{Communicator, ThreadCommunicator};
use progress::timestamp::RootTimestamp;

pub trait GraphBuilder: Sized {
    type Timestamp : Timestamp;
    type Communicator : Communicator;

    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64;
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T>;

    fn communicator(&mut self) -> &mut Self::Communicator;
    fn name(&self) -> String;

    fn new_subgraph<'a, T: Timestamp>(&'a mut self) -> SubgraphBuilder<&'a mut Self, T> {
        let subscope = self.new_subscope();
        SubgraphBuilder {
            subgraph: subscope,
            parent: self
        }
    }
}

impl<'a, G: GraphBuilder> GraphBuilder for &'a mut G {
    type Timestamp = G::Timestamp;
    type Communicator = G::Communicator;

    fn connect(&mut self, source: Source, target: Target) { (**self).connect(source, target) }
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64 { (**self).add_boxed_scope(scope) }
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T> { (**self).new_subscope() }
    fn name(&self) -> String { (**self).name() }

    fn communicator(&mut self) -> &mut Self::Communicator { (**self).communicator() }
}

pub struct GraphRoot<C: Communicator> {
    communicator:   C,
    graph:          Option<Box<Scope<RootTimestamp>>>,
}

impl<C: Communicator> GraphRoot<C> {
    pub fn new(c: C) -> GraphRoot<C> {
        GraphRoot { communicator: c, graph: None }
    }
    pub fn step(&mut self) -> bool {
        if let Some(ref mut scope) = self.graph {
            scope.pull_internal_progress(&mut [], &mut [], &mut [])
        }
        else { panic!("GraphRoot::step(): empty; make sure to add a subgraph!") }
    }
}

impl<C: Communicator> GraphBuilder for GraphRoot<C> {
    type Timestamp = RootTimestamp;
    type Communicator = C;

    fn connect(&mut self, _source: Source, _target: Target) {
        panic!("GraphRoot::connect(): root doesn't maintain edges; who are you, how did you get here?")
    }
    fn add_boxed_scope(&mut self, mut scope: Box<Scope<RootTimestamp>>) -> u64  {
        if self.graph.is_some() { panic!("GraphRoot::add_boxed_scope(): added second scope to root") }
        else                    {
            scope.get_internal_summary();
            scope.set_external_summary(Vec::new(), &mut []);
            self.graph = Some(scope);
            0
        }
    }

    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<RootTimestamp, T>  {
        let name = format!("{}::Subgraph[0]", self.name());
        Subgraph::new_from(&mut self.communicator, 0, name)
    }

    fn communicator(&mut self) -> &mut C { &mut self.communicator }
    fn name(&self) -> String { format!("Root") }
}



pub struct SubgraphBuilder<G: GraphBuilder, T: Timestamp> {
    pub subgraph: Subgraph<G::Timestamp, T>,
    pub parent:   G,
}

impl<G: GraphBuilder, T: Timestamp> Drop for SubgraphBuilder<G, T> {
    fn drop(&mut self) {
        // TODO : This is a pretty silly way to grab the subgraph. perhaps something more tasteful?
        let subgraph = mem::replace(&mut self.subgraph, Subgraph::new_from(&mut ThreadCommunicator, 0, format!("Empty")));
        self.parent.add_scope(subgraph);
    }
}

impl<G: GraphBuilder, T: Timestamp> GraphBuilder for SubgraphBuilder<G, T>{
    type Timestamp = Product<G::Timestamp, T>;
    type Communicator = G::Communicator;

    fn connect(&mut self, source: Source, target: Target) {
        self.subgraph.connect(source, target);
    }
    fn add_boxed_scope(&mut self, scope: Box<Scope<Product<G::Timestamp, T>>>) -> u64 {
        let index = self.subgraph.children.len() as u64;
        let name = self.name();
        self.subgraph.children.push(ScopeWrapper::new(scope, index, name));
        index
    }

    fn new_subscope<T2: Timestamp>(&mut self) -> Subgraph<Product<G::Timestamp, T>, T2> {
        let name = format!("{}::Subgraph[{}]", self.name(), self.subgraph.children());
        Subgraph::new_from(self.parent.communicator(), self.subgraph.children() as u64, name)
    }

    fn communicator(&mut self) -> &mut G::Communicator { self.parent.communicator() }
    fn name(&self) -> String { self.subgraph.name() }
}
