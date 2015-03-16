use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use progress::{Timestamp, Graph, CountMap};
use progress::subgraph::Source::{GraphInput, ScopeOutput};
use progress::subgraph::Target::{GraphOutput, ScopeInput};
use progress::subgraph::Subgraph;

use example::stream::Stream;
use communication::{Observer, Communicator};
use communication::channels::{Data, OutputPort, ObserverHelper};

pub trait EnterSubgraphExt<TOuter: Timestamp, GInner: Graph<Timestamp=(TOuter, TInner)>, TInner: Timestamp, D: Data, C: Communicator> {
    fn enter(&mut self, subgraph: &Rc<RefCell<Subgraph<TOuter, TInner>>>) -> Stream<GInner, D, C>;
}

impl<GOuter: Graph, TInner: Timestamp, D: Data, C: Communicator> EnterSubgraphExt<GOuter::Timestamp, Rc<RefCell<Subgraph<GOuter::Timestamp, TInner>>>, TInner, D, C> for Stream<GOuter, D, C> {
    fn enter(&mut self, subgraph: &Rc<RefCell<Subgraph<GOuter::Timestamp, TInner>>>) -> Stream<Rc<RefCell<Subgraph<GOuter::Timestamp, TInner>>>, D, C> {

        let targets = OutputPort::<(GOuter::Timestamp, TInner), D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let scope_index = subgraph.borrow().index;
        let input_index = subgraph.borrow_mut().new_input(produced);

        self.connect_to(ScopeInput(scope_index, input_index), ingress);

        Stream {
            name: GraphInput(input_index),
            ports: targets,
            graph: subgraph.clone(),
            allocator: self.allocator.clone()
        }
    }
}

pub trait LeaveSubgraphExt<GOuter: Graph, D: Data, C: Communicator> {
    fn leave(&mut self, graph: &GOuter) -> Stream<GOuter, D, C>;
}

impl<GOuter: Graph, TInner: Timestamp, D: Data, C: Communicator> LeaveSubgraphExt<GOuter, D, C> for Stream<Rc<RefCell<Subgraph<GOuter::Timestamp, TInner>>>, D, C> {
    fn leave(&mut self, graph: &GOuter) -> Stream<GOuter, D, C> {

        let index = self.graph.borrow_mut().new_output();
        let targets = OutputPort::<GOuter::Timestamp, D>::new();

        self.connect_to(GraphOutput(index), EgressNub { targets: targets.clone(), phantom: PhantomData });

        Stream {
            name: ScopeOutput(self.graph.borrow_mut().index, index),
            ports: targets,
            graph: graph.clone(),
            allocator: self.allocator.clone()
        }

    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: ObserverHelper<OutputPort<(TOuter, TInner), TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer for IngressNub<TOuter, TInner, TData> {
    type Time = TOuter;
    type Data = TData;
    #[inline(always)] fn push(&mut self, data: &TData) { self.targets.push(data); }
    #[inline(always)] fn open(&mut self, time: &TOuter) -> () { self.targets.open(&(time.clone(), Default::default())); }
    #[inline(always)] fn shut(&mut self, time: &TOuter) -> () { self.targets.shut(&(time.clone(), Default::default())); }
}


pub struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: OutputPort<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Observer for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    type Time = (TOuter, TInner);
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &(TOuter, TInner)) { self.targets.open(&time.0); }
    #[inline(always)] fn push(&mut self, data: &TData) { self.targets.push(data); }
    #[inline(always)] fn shut(&mut self, time: &(TOuter, TInner)) { self.targets.shut(&time.0); }
}
