use std::rc::Rc;
use std::cell::RefCell;

use progress::timestamp::RootTimestamp;
use progress::{Timestamp, Operate, Subgraph};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use progress::nested::scope_wrapper::ScopeWrapper;
use fabric::{Allocate, Data, Push, Pull};
// use communication::Data;
// use serialization::Serializable;

/// The fundamental operations required to add and connect operators in a timely dataflow graph.
///
/// Importantly, this is often a *shared* object, backed by a `Rc<RefCell<>>` wrapper. Each method
/// takes a shared reference, but can be thought of as first calling .clone() and then calling the
/// method. Each method does not hold the `RefCell`'s borrow, and should prevent accidental panics.
pub trait GraphBuilder : Allocate+Clone {
    type Timestamp : Timestamp;

    /// A useful name describing the builder's scope.
    fn name(&self) -> String;

    /// Connects a source of data with a target of the data. This only links the two for
    /// the purposes of tracking progress, rather than effect any data movement itself.
    fn add_edge(&self, source: Source, target: Target);

    /// Adds a child `Operate` to the builder's scope. Returns the new child's index.
    fn add_operator<SC: Operate<Self::Timestamp>+'static>(&self, scope: SC) -> usize;

    /// Creates a new `Subgraph` with timestamp `T`. Used by `subcomputation`, but unlikely to be
    /// commonly useful to end users.
    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T>;

    /// Creates a `Subgraph` from a closure acting on a `SubgraphBuilder`, and returning
    /// whatever the closure returns.
    ///
    /// Commonly used to create new timely dataflow subgraphs, either creating new input streams
    /// and the input handle, or ingressing data streams and returning the egresses stream.
    ///
    /// # Examples
    /// ```
    /// use timely::construction::*;
    /// use timely::construction::operators::*;
    ///
    /// timely::execute(std::env::args(), |root| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = root.subcomputation::<u64,_,_>(|subgraph| {
    ///         let (input, stream) = subgraph.new_input::<String>();
    ///         let output = subgraph.subcomputation::<u32,_,_>(|subgraph2| {
    ///             subgraph2.enter(&stream).leave()
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    fn subcomputation<T: Timestamp, R, F:FnOnce(&mut SubgraphBuilder<Self, T>)->R>(&mut self, func: F) -> R {
        let subscope = Rc::new(RefCell::new(self.new_subscope()));
        let mut builder = SubgraphBuilder {
            subgraph: subscope,
            parent: self.clone(),
        };

        let result = func(&mut builder);
        self.add_operator(builder.subgraph);
        result
    }
}

/// A `GraphRoot` is the entry point to a timely dataflow computation. It wraps a `Allocate`,
/// and has a slot for one child `Operate`. The primary intended use of `GraphRoot` is through its
/// implementation of the `GraphBuilder` trait.
///
/// # Panics
/// Calling `subcomputation` more than once will result in a `panic!`.
///
/// Calling `step` without having called `subcomputation` will result in a `panic!`.
pub struct GraphRoot<A: Allocate> {
    allocator: Rc<RefCell<A>>,
    graph: Rc<RefCell<Option<Box<Operate<RootTimestamp>>>>>,
}

impl<A: Allocate> GraphRoot<A> {
    pub fn new(c: A) -> GraphRoot<A> {
        GraphRoot {
            allocator: Rc::new(RefCell::new(c)),
            graph: Rc::new(RefCell::new(None)),
        }
    }
    pub fn step(&mut self) -> bool {
        if let Some(scope) = self.graph.borrow_mut().as_mut() {
            scope.pull_internal_progress(&mut [], &mut [], &mut [])
        }
        else { panic!("GraphRoot::step(): empty; make sure to add a subgraph!") }
    }
    pub fn index(&self) -> usize { self.allocator.borrow().index() }
    pub fn peers(&self) -> usize { self.allocator.borrow().peers() }
}

impl<A: Allocate> GraphBuilder for GraphRoot<A> {
    type Timestamp = RootTimestamp;

    fn name(&self) -> String { format!("Worker[{}]", self.allocator.borrow().index()) }
    fn add_edge(&self, _source: Source, _target: Target) {
        panic!("GraphRoot::connect(): root doesn't maintain edges; who are you, how did you get here?")
    }

    fn add_operator<SC: Operate<RootTimestamp>+'static>(&self, mut scope: SC) -> usize  {
        let mut borrow = self.graph.borrow_mut();
        if borrow.is_none() {
            scope.get_internal_summary();
            scope.set_external_summary(Vec::new(), &mut []);
            *borrow = Some(Box::new(scope));
            0
        }
        else { panic!("GraphRoot::add_operator(): added second scope to root") }
    }

    fn new_subscope<T: Timestamp>(&mut self) -> Subgraph<RootTimestamp, T>  {
        let name = format!("{}::Subgraph[Root]", self.name());
        Subgraph::new_from(&mut (*self.allocator.borrow_mut()), 0, name)
    }
}

impl<A: Allocate> Allocate for GraphRoot<A> {
    fn index(&self) -> usize { self.allocator.borrow().index() }
    fn peers(&self) -> usize { self.allocator.borrow().peers() }
    fn allocate<D: Data>(&mut self) -> (Vec<Box<Push<D>>>, Box<Pull<D>>) {
        self.allocator.borrow_mut().allocate()
    }
}

impl<A: Allocate> Clone for GraphRoot<A> {
    fn clone(&self) -> Self { GraphRoot { allocator: self.allocator.clone(), graph: self.graph.clone() }}
}

/// A `SubgraphBuilder` wraps a `Subgraph` and a parent `G: GraphBuilder`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct SubgraphBuilder<G: GraphBuilder, T: Timestamp> {
    pub subgraph: Rc<RefCell<Subgraph<G::Timestamp, T>>>,
    pub parent:   G,
}

impl<G: GraphBuilder, T: Timestamp> GraphBuilder for SubgraphBuilder<G, T> {
    type Timestamp = Product<G::Timestamp, T>;

    fn name(&self) -> String { self.subgraph.borrow().name().to_owned() }
    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_operator<SC: Operate<Self::Timestamp>+'static>(&self, scope: SC) -> usize {
        let index = self.subgraph.borrow().children.len();
        let path = format!("{}", self.subgraph.borrow().path);
        self.subgraph.borrow_mut().children.push(ScopeWrapper::new(Box::new(scope), index, path));
        index
    }

    fn new_subscope<T2: Timestamp>(&mut self) -> Subgraph<Product<G::Timestamp, T>, T2> {
        let index = self.subgraph.borrow().children();
        let path = format!("{}", self.subgraph.borrow().path);
        Subgraph::new_from(self, index, path)
    }
}

impl<G: GraphBuilder, T: Timestamp> Allocate for SubgraphBuilder<G, T> {
    fn index(&self) -> usize { self.parent.index() }
    fn peers(&self) -> usize { self.parent.peers() }
    fn allocate<D:Data>(&mut self) -> (Vec<Box<Push<D>>>, Box<Pull<D>>) {
        self.parent.allocate()
    }
}

impl<G: GraphBuilder, T: Timestamp> Clone for SubgraphBuilder<G, T> {
    fn clone(&self) -> Self { SubgraphBuilder { subgraph: self.subgraph.clone(), parent: self.parent.clone() }}
}
