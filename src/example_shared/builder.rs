use std::rc::Rc;
use std::cell::RefCell;
use std::any::Any;

use progress::{Timestamp, Scope, Subgraph};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use progress::nested::scope_wrapper::ScopeWrapper;
use communication::{Pushable, Pullable, Communicator};
use progress::timestamp::RootTimestamp;

use columnar::Columnar;

/// GraphBuilder provides the fundamental operations required for connecting operators in a timely
/// dataflow graph.

/// Importantly, this is a *shared* object, almost certainly backed by a Rc<RefCell<>> wrapper.
/// Each method is designed so that it does not hold the RefCell's borrow, meaning you shouldn't be
/// able to shoot yourself in the foot. Each method takes a shared borrow, and can be thought of as
/// first calling .clone() and then calling the method.

pub trait GraphBuilder : Communicator+Clone {
    type Timestamp : Timestamp;

    fn name(&self) -> String;

    fn add_edge(&self, source: Source, target: Target);

    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&self, scope: SC) -> u64;  // returns name

    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T>;

    // TODO : Learn about the scoped() pattern that prevents the subgraph builder from escaping
    fn subcomputation<T: Timestamp, R, F:FnOnce(&SubgraphBuilder<Self, T>)->R>(&mut self, func: F) -> R {
        let subscope = Rc::new(RefCell::new(self.new_subscope()));
        let builder = SubgraphBuilder {
            subgraph: subscope,
            parent: self.clone(),
        };

        let result = func(&builder);

        self.add_scope(builder.subgraph);

        result
        //
        //
        // if let Ok(subgraph) = try_unwrap(builder.subgraph) {
        //     self.add_scope(subgraph.into_inner());
        //     result
        // }
        // else {
        //     panic!("subcomputation failed to get unique handle to subgraph builder");
        // }
    }
}

// impl<G: GraphBuilder> GraphBuilder for Rc<RefCell<G>> {
//     type Timestamp = G::Timestamp;
//
//     fn name(&self) -> String { self.borrow().name() }
//     fn add_edge(&self, source: Source, target: Target) { self.borrow().add_edge(source, target) }
//     fn add_scope<SC: Scope<Self::Timestamp>+'static>(&self, scope: SC) { self.borrow().add_scope() }
//     fn new_subscope<T: Timestamp>(&self) -> Subgraph<Self::Timestamp, T> { self.borrow().new_subscope() }
// }

pub struct GraphRoot<C: Communicator> {
    communicator:   Rc<RefCell<C>>,
    graph:          Rc<RefCell<Option<Box<Scope<RootTimestamp>>>>>,
}

impl<C: Communicator> GraphRoot<C> {
    pub fn new(c: C) -> GraphRoot<C> {
        GraphRoot {
            communicator: Rc::new(RefCell::new(c)),
            graph:        Rc::new(RefCell::new(None)),
        }
    }
    pub fn step(&mut self) -> bool {
        if let Some(scope) = self.graph.borrow_mut().as_mut() {
            scope.pull_internal_progress(&mut [], &mut [], &mut [])
        }
        else { panic!("GraphRoot::step(): empty; make sure to add a subgraph!") }
    }
}

impl<C: Communicator> GraphBuilder for GraphRoot<C> {
    type Timestamp = RootTimestamp;

    fn name(&self) -> String { format!("Root") }
    fn add_edge(&self, _source: Source, _target: Target) {
        panic!("GraphRoot::connect(): root doesn't maintain edges; who are you, how did you get here?")
    }

    fn add_scope<SC: Scope<RootTimestamp>+'static>(&self, mut scope: SC) -> u64  {
        let mut borrow = self.graph.borrow_mut();
        if borrow.is_none() {
            scope.get_internal_summary();
            scope.set_external_summary(Vec::new(), &mut []);
            *borrow = Some(Box::new(scope));
            0
        }
        else { panic!("GraphRoot::add_scope(): added second scope to root") }
    }

    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<RootTimestamp, T>  {
        let name = format!("{}::Subgraph[0]", self.name());
        Subgraph::new_from(&mut (*self.communicator.borrow_mut()), 0, name)
    }
}

impl<C: Communicator> Communicator for GraphRoot<C> {
    fn index(&self) -> u64 { self.communicator.borrow().index() }
    fn peers(&self) -> u64 { self.communicator.borrow().peers() }
    fn new_channel<T:Send+Columnar+Any>(&mut self) -> (Vec<Box<Pushable<T>>>, Box<Pullable<T>>) {
        self.communicator.borrow_mut().new_channel()
    }
}

impl<C: Communicator> Clone for GraphRoot<C> {
    fn clone(&self) -> Self { GraphRoot { communicator: self.communicator.clone(), graph: self.graph.clone() }}
}


pub struct SubgraphBuilder<G: GraphBuilder, T: Timestamp> {
    pub subgraph: Rc<RefCell<Subgraph<G::Timestamp, T>>>,
    pub parent:   G,
}

// impl<G: GraphBuilder, T: Timestamp> Drop for SubgraphBuilder<G, T> {
//     fn drop(&mut self) {
//         // TODO : This is a pretty silly way to grab the subgraph. perhaps something more tasteful?
//         let subgraph = mem::replace(&mut self.subgraph, Subgraph::new_from(&mut ThreadCommunicator, 0, format!("Empty")));
//         self.parent.add_scope(subgraph);
//     }
// }

impl<G: GraphBuilder, T: Timestamp> GraphBuilder for SubgraphBuilder<G, T> {
    type Timestamp = Product<G::Timestamp, T>;

    fn name(&self) -> String { self.subgraph.borrow().name() }
    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&self, scope: SC) -> u64 {
        let index = self.subgraph.borrow().children.len() as u64;
        let name = format!("{}", self.name());
        self.subgraph.borrow_mut().children.push(ScopeWrapper::new(Box::new(scope), index, name));
        index
    }

    fn new_subscope<T2: Timestamp>(&mut self) -> Subgraph<Product<G::Timestamp, T>, T2> {
        let index = self.subgraph.borrow().children() as u64;
        let name = format!("{}::Subgraph[{}]", self.name(), index);
        Subgraph::new_from(self, index, name)
    }
}

impl<G: GraphBuilder, T: Timestamp> Communicator for SubgraphBuilder<G, T> {
    fn index(&self) -> u64 { self.parent.index() }
    fn peers(&self) -> u64 { self.parent.peers() }
    fn new_channel<D:Send+Columnar+Any>(&mut self) -> (Vec<Box<Pushable<D>>>, Box<Pullable<D>>) {
        self.parent.new_channel()
    }
}

impl<G: GraphBuilder, T: Timestamp> Clone for SubgraphBuilder<G, T> {
    fn clone(&self) -> Self { SubgraphBuilder { subgraph: self.subgraph.clone(), parent: self.parent.clone() }}
}
