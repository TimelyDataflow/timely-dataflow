use std::mem;

use progress::{Timestamp, Scope, Subgraph};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use progress::nested::scope_wrapper::ScopeWrapper;
use communication::{Communicator, ThreadCommunicator};

pub trait GraphBuilder {
    type Timestamp : Timestamp;
    type Communicator : Communicator;

    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64;
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subgraph<T: Timestamp>(&mut self) -> SubgraphBuilder<Self, T>;

    fn communicator(&mut self) -> &mut Self::Communicator;
}

pub struct Root<C: Communicator> {
    communicator:   C,
    graph:          Option<Box<Scope<()>>>,
}

impl<C: Communicator> Root<C> {
    pub fn new(c: C) -> Root<C> {
        Root { communicator: c, graph: None }
    }
    pub fn step(&mut self) -> bool {
        if let Some(ref mut scope) = self.graph {
            scope.pull_internal_progress(&mut [], &mut [], &mut [])
        }
        else { panic!("Root empty; make sure to add a subgraph!") }
    }
}

impl<C: Communicator> GraphBuilder for Root<C> {
    type Timestamp = ();
    type Communicator = C;

    fn connect(&mut self, _source: Source, _target: Target) {
        panic!("root doesn't maintain edges; who are you, how did you get here?")
    }
    fn add_boxed_scope(&mut self, mut scope: Box<Scope<()>>) -> u64  {
        if self.graph.is_some() { panic!("added two scopes to root") }
        else                    {
            scope.get_internal_summary();
            scope.set_external_summary(Vec::new(), &mut []);
            self.graph = Some(scope);
            0
        }
    }
    fn add_scope<SC: Scope<()>+'static>(&mut self, scope: SC) -> u64 {
        self.add_boxed_scope(Box::new(scope))
    }
    fn new_subgraph<T: Timestamp>(&mut self) -> SubgraphBuilder<Self, T>  {
        SubgraphBuilder {
            subgraph: Subgraph::<(), T>::new_from(&mut self.communicator, 0),
            parent:   self,
        }
    }

    fn communicator(&mut self) -> &mut C { &mut self.communicator }
}



pub struct SubgraphBuilder<'a, G: GraphBuilder+'a, T: Timestamp> {
    pub subgraph: Subgraph<G::Timestamp, T>,
    pub parent:   &'a mut G,
}

// impl<'a, G: GraphBuilder+'a, T: Timestamp> SubgraphBuilder<'a, G, T>{
//     pub fn seal(self) {
//         self.parent.add_scope(self.subgraph);
//     }
// }

impl<'a, G: GraphBuilder+'a, T: Timestamp> Drop for SubgraphBuilder<'a, G, T> {
    fn drop(&mut self) {
        let subgraph = mem::replace(&mut self.subgraph, Subgraph::new_from(&mut ThreadCommunicator, 0));
        // println!("adding {} to parent", subgraph.name());
        self.parent.add_scope(subgraph);
    }
}

impl<'a, G: GraphBuilder+'a, T: Timestamp> GraphBuilder for SubgraphBuilder<'a, G, T>{
    type Timestamp = Product<G::Timestamp, T>;
    type Communicator = G::Communicator;

    fn connect(&mut self, source: Source, target: Target) {
        self.subgraph.connect(source, target);
    }
    fn add_boxed_scope(&mut self, scope: Box<Scope<Product<G::Timestamp, T>>>) -> u64 {
        let index = self.subgraph.children.len() as u64;
        self.subgraph.children.push(ScopeWrapper::new(scope, index));
        index
    }

    fn new_subgraph<T2: Timestamp>(&mut self) -> SubgraphBuilder<Self, T2> {
        SubgraphBuilder {
            subgraph: Subgraph::new_from(self.parent.communicator(), self.subgraph.children() as u64),
            parent:   self,
        }
    }

    fn communicator(&mut self) -> &mut G::Communicator { self.parent.communicator() }
}
