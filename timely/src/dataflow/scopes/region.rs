//! A child dataflow scope used to build optional dataflow regions.

use crate::{
    communication::{
        allocator::thread::{ThreadPuller, ThreadPusher},
        Data, Pull, Push,
    },
    dataflow::{scopes::Child, Scope, ScopeParent},
    logging::{TimelyLogger as Logger, TimelyProgressLogger as ProgressLogger, WorkerIdentifier},
    logging_core::Registry,
    progress::{timestamp::Refines, Operate, Source, SubgraphBuilder, Target, Timestamp},
    scheduling::{Activations, Scheduler},
    worker::{AsWorker, Config},
};
use std::{
    cell::{RefCell, RefMut},
    rc::Rc,
};
use timely_communication::Message;

type AllocatedChannels<D> = (Vec<Box<dyn Push<Message<D>>>>, Box<dyn Pull<Message<D>>>);

/// A nested dataflow region, can either be a subgraph equivalent to [`Scope::region()`]
/// or a no-op that has no affect on the dataflow graph
pub struct Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    /// The subgraph under construction
    pub subgraph: RegionSubgraph<'a, G, T>,
    /// A copy of the child's parent scope.
    pub parent: G,
    /// The log writer for this scope.
    pub logging: Option<Logger>,
    /// The progress log writer for this scope.
    pub progress_logging: Option<ProgressLogger>,
}

impl<'a, G, T> Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    fn allocate_child_id(&mut self) -> usize {
        match self.subgraph {
            RegionSubgraph::Subgraph { subgraph, .. } => subgraph.borrow_mut().allocate_child_id(),
            RegionSubgraph::Passthrough => self.parent.allocate_operator_index(),
        }
    }

    fn path(&self) -> Vec<usize> {
        match self.subgraph {
            RegionSubgraph::Subgraph { subgraph, .. } => subgraph.borrow().path.clone(),
            RegionSubgraph::Passthrough => self.parent.addr(),
        }
    }

    fn create_child_subscope<TOuter, TInner>(
        &mut self,
        name: &str,
    ) -> RefCell<SubgraphBuilder<TOuter, TInner>>
    where
        TOuter: Timestamp,
        TInner: Timestamp + Refines<TOuter>,
    {
        let index = self.allocate_child_id();
        let path = self.path();

        RefCell::new(SubgraphBuilder::new_from(
            index,
            path,
            self.logging(),
            self.progress_logging.clone(),
            name,
        ))
    }
}

impl<'a, G, T> AsWorker for Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    fn config(&self) -> &Config {
        self.parent.config()
    }

    fn index(&self) -> usize {
        self.parent.index()
    }

    fn peers(&self) -> usize {
        self.parent.peers()
    }

    fn allocate<D: Data>(&mut self, identifier: usize, address: &[usize]) -> AllocatedChannels<D> {
        self.parent.allocate(identifier, address)
    }

    fn pipeline<D: 'static>(
        &mut self,
        identifier: usize,
        address: &[usize],
    ) -> (ThreadPusher<Message<D>>, ThreadPuller<Message<D>>) {
        self.parent.pipeline(identifier, address)
    }

    fn new_identifier(&mut self) -> usize {
        self.parent.new_identifier()
    }

    fn log_register(&self) -> RefMut<Registry<WorkerIdentifier>> {
        self.parent.log_register()
    }
}

impl<'a, G, T> Scheduler for Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.parent.activations()
    }
}

impl<'a, G, T> ScopeParent for Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    type Timestamp = T;
}

impl<'a, G, T> Scope for Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Refines<G::Timestamp>,
{
    fn name(&self) -> String {
        todo!()
    }

    fn addr(&self) -> Vec<usize> {
        todo!()
    }

    fn add_edge(&self, source: Source, target: Target) {
        match self.subgraph {
            RegionSubgraph::Subgraph { subgraph, .. } => {
                subgraph.borrow_mut().connect(source, target);
            }
            RegionSubgraph::Passthrough => self.parent.add_edge(source, target),
        }
    }

    fn allocate_operator_index(&mut self) -> usize {
        match self.subgraph {
            RegionSubgraph::Subgraph { subgraph, .. } => subgraph.borrow_mut().allocate_child_id(),
            RegionSubgraph::Passthrough => self.parent.allocate_operator_index(),
        }
    }

    fn add_operator_with_indices(
        &mut self,
        operator: Box<dyn Operate<Self::Timestamp>>,
        local: usize,
        global: usize,
    ) {
        match self.subgraph {
            RegionSubgraph::Subgraph { subgraph, .. } => {
                subgraph.borrow_mut().add_child(operator, local, global);
            }
            RegionSubgraph::Passthrough => self
                .parent
                .add_operator_with_indices(operator, local, global),
        }
    }

    fn scoped<T2, R, F>(&mut self, name: &str, func: F) -> R
    where
        T2: Timestamp + Refines<Self::Timestamp>,
        F: FnOnce(&mut Child<Self, T2>) -> R,
    {
        let subscope = self.create_child_subscope(name);
        let index = subscope.borrow().index();

        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
                progress_logging: self.progress_logging.clone(),
            };
            func(&mut builder)
        };

        let subscope = subscope.into_inner().build(self);
        self.add_operator_with_index(Box::new(subscope), index);

        result
    }

    fn optional_region_named<R, F>(&mut self, name: &str, enabled: bool, func: F) -> R
    where
        F: FnOnce(&mut Region<Self, Self::Timestamp>) -> R,
    {
        // If the region is enabled then build the child dataflow graph, otherwise
        // create a passthrough region
        let region = if enabled {
            Some(self.create_child_subscope(name))
        } else {
            None
        };

        let result = {
            let region = region
                .as_ref()
                .map_or(RegionSubgraph::Passthrough, |subscope| {
                    let index = subscope.borrow().index();
                    RegionSubgraph::subgraph(subscope, index)
                });

            let mut builder = Region {
                subgraph: region,
                parent: self.clone(),
                logging: self.logging.clone(),
                progress_logging: self.progress_logging.clone(),
            };
            func(&mut builder)
        };

        // If the region is enabled then build and add the sub-scope to the dataflow graph
        if let Some(subscope) = region {
            let index = subscope.borrow().index();
            let subscope = subscope.into_inner().build(self);

            self.add_operator_with_index(Box::new(subscope), index);
        }

        result
    }
}

impl<'a, G, T> Clone for Region<'a, G, T>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            subgraph: self.subgraph,
            parent: self.parent.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone(),
        }
    }
}

/// The kind of region to build, can either be a subgraph equivalent to [`Scope::region()`]
/// or a no-op that has no affect on the dataflow graph
pub enum RegionSubgraph<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp,
{
    /// A region that will be rendered as a nested dataflow scope
    Subgraph {
        /// The inner dataflow scope
        subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>,
        /// The subgraph's operator index
        index: usize,
    },
    /// A region that will be a no-op in the dataflow graph
    Passthrough,
}

impl<'a, G, T> RegionSubgraph<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp,
{
    /// Create a new region subgraph
    pub(crate) fn subgraph(
        subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>,
        index: usize,
    ) -> Self {
        Self::Subgraph { subgraph, index }
    }
}

impl<'a, G, T> Clone for RegionSubgraph<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        match *self {
            Self::Subgraph { subgraph, index } => Self::Subgraph { subgraph, index },
            Self::Passthrough => Self::Passthrough,
        }
    }
}

impl<'a, G, T> Copy for RegionSubgraph<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp,
{
}
