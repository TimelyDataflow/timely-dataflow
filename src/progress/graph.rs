use std::cell::RefCell;
use progress::{Timestamp, Scope, Subgraph};
use progress::nested::{Source, Target};
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
