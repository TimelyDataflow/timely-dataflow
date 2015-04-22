use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Scope, Subgraph, CountMap};
use progress::nested::{Source, Target};
use progress::nested::product::Product;
use progress::nested::scope_wrapper::ScopeWrapper;

use progress::frontier::MutableAntichain;

use communication::Communicator;
use progress::timestamp::RootTimestamp;

pub trait Graph {
    type Timestamp : Timestamp;
    type Communicator : Communicator;

    fn connect(&mut self, source: Source, target: Target);
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64;
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T>;

    fn with_communicator<R, F: FnOnce(&mut Self::Communicator)->R>(&mut self, func: F) -> R;

    fn builder(&mut self) -> RefCell<&mut Self> { RefCell::new(self) }
}

impl<'a, G: Graph+'a> Graph for &'a mut G {
    type Timestamp = G::Timestamp;
    type Communicator = G::Communicator;

    fn connect(&mut self, source: Source, target: Target) { (**self).connect(source, target) }
    fn add_boxed_scope(&mut self, scope: Box<Scope<Self::Timestamp>>) -> u64 { (**self).add_boxed_scope(scope) }
    fn add_scope<SC: Scope<Self::Timestamp>+'static>(&mut self, scope: SC) -> u64 { self.add_boxed_scope(Box::new(scope)) }
    fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<Self::Timestamp, T> { (**self).new_subgraph() }

    fn with_communicator<R, F: FnOnce(&mut Self::Communicator)->R>(&mut self, func: F) -> R { (**self).with_communicator(func) }
}


pub struct Root<C: Communicator> {
    communicator:   C,
    graph:          Option<Box<Scope<RootTimestamp>>>,
}

impl<C: Communicator> Root<C> {
    pub fn new(c: C) -> Root<C> {
        Root {
            communicator: c,
            graph: None,
        }
    }
    pub fn step(&mut self) -> bool {
        if let Some(ref mut scope) = self.graph {
            scope.pull_internal_progress(&mut [], &mut [], &mut [])
        }
        else { panic!("Root empty; make sure to add a subgraph!") }
    }
}

impl<C: Communicator> Graph for Root<C> {
    type Timestamp = RootTimestamp;
    type Communicator = C;

    fn connect(&mut self, _source: Source, _target: Target) {
        panic!("root doesn't maintain edges; who are you, how did you get here?")
    }
    fn add_boxed_scope(&mut self, mut scope: Box<Scope<RootTimestamp>>) -> u64  {
        if self.graph.is_some() { panic!("added two scopes to root") }
        else                    {
            scope.get_internal_summary();
            scope.set_external_summary(Vec::new(), &mut []);
            self.graph = Some(scope);
            0
        }
    }
    fn add_scope<SC: Scope<RootTimestamp>+'static>(&mut self, scope: SC) -> u64 {
        self.add_boxed_scope(Box::new(scope))
    }
    fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<RootTimestamp, T>  {
        Subgraph::<RootTimestamp, T>::new_from(&mut self.communicator, 0, format!("Root"))
    }

    fn with_communicator<R, F: FnOnce(&mut Self::Communicator)->R>(&mut self, func: F) -> R {
        func(&mut self.communicator)
    }
}

// Builder is intended to act as a guard for subgraph assembly.
// It wraps a subgraph and a reference to its parent, proxying many of their methods.
// At disposal time it is capable of adding to the parent the subgraph boxed as a scope.
// This might be do-able with Drop, but it currently ICEs. Instead, called seal().

// TODO : It may be more ergonomic to have parent: RefCell<&'b mut G>, owning the reference.
// TODO : This avoids a second object and scope when writing code, but introduces the issue that
// TODO : by owning the &'b mut G outer uses of an &'a RefCell<&'b mut G> result in runtime panics.
// TODO : In particular, this requires care on scope entrance, so that the scope does the logic
// TODO : rather than the stream itself, whose Rc<&'b mut G> may be acquired by the scope.

// TODO : It seems plausible that the Graph interface could expose only methods that do not borrow,
// TODO : so that any .borrow_mut().method() would avoid runtime panics if we did not do the above.

pub struct SubgraphBuilder<'a, G: Graph+'a, TInner: Timestamp> {
    subgraph: Subgraph<G::Timestamp, TInner>,
    parent:   &'a RefCell<G>,
}

impl<'a, G: Graph+'a, TInner: Timestamp> SubgraphBuilder<'a, G, TInner> {
    pub fn new(parent: &'a RefCell<G>) -> SubgraphBuilder<'a, G, TInner> {
        let subgraph = parent.borrow_mut().new_subgraph::<TInner>();
        SubgraphBuilder {
            subgraph: subgraph,
            parent: parent,
        }
    }
    pub fn index(&self) -> u64 { self.subgraph.index }
    pub fn parent(&self) -> &'a RefCell<G> { self.parent }
    pub fn new_input(&mut self, shared_counts: Rc<RefCell<CountMap<Product<G::Timestamp, TInner>>>>) -> u64 {
        self.subgraph.inputs += 1;
        self.subgraph.external_guarantee.push(MutableAntichain::new());
        self.subgraph.input_messages.push(shared_counts);
        return self.subgraph.inputs - 1;
    }

    pub fn new_output(&mut self) -> u64 {
        self.subgraph.outputs += 1;
        self.subgraph.external_capability.push(MutableAntichain::new());
        return self.subgraph.outputs - 1;
    }

    pub fn seal(self) {
        // TODO : Comment me out for ICE (step 1/2)
        self.parent.borrow_mut().add_scope(self.subgraph);
    }
}

impl<'a, TInner: Timestamp, G: Graph+'a> Graph for SubgraphBuilder<'a, G, TInner> {
    type Timestamp = Product<G::Timestamp, TInner>;
    type Communicator = G::Communicator;

    fn connect(&mut self, source: Source, target: Target) {
        self.subgraph.connect(source, target);
    }
    fn add_boxed_scope(&mut self, scope: Box<Scope<Product<G::Timestamp, TInner>>>) -> u64 {
        let index = self.subgraph.children.len() as u64;
        self.subgraph.children.push(ScopeWrapper::new(scope, index));
        index
    }

    fn new_subgraph<T: Timestamp>(&mut self) -> Subgraph<Product<G::Timestamp, TInner>, T> {
        let index = self.subgraph.children() as u64;
        self.parent.borrow_mut().with_communicator(|x| Subgraph::new_from(x, index, format!("Subgraph-{}", index)))
    }

    fn with_communicator<R, F: FnOnce(&mut Self::Communicator)->R>(&mut self, func: F) -> R {
        self.parent.borrow_mut().with_communicator(func)
    }
}

// TODO : Uncomment me for ICE (step 2/2)
// impl<'a, 'b: 'a, TInner: Timestamp, G: Graph+'b> Drop for SubgraphBuilder<'a, 'b, G, TInner> {
//     fn drop(&mut self) { self.parent.borrow_mut().add_scope(self.subgraph.take().unwrap()); }
// }

