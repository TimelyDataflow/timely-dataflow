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

pub trait EnterSubgraphExt<TOuter: Timestamp, TInner: Timestamp, D: Data, C: Communicator> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut Subgraph<TOuter, TInner>>, communicator: C) -> Stream<(&'a RefCell<&'b mut Subgraph<TOuter, TInner>>, C), D>;
}

impl<GOuter: Graph, TInner: Timestamp, D: Data, C: Communicator + Clone> EnterSubgraphExt<GOuter::Timestamp, TInner, D, C> for Stream<GOuter, D> {
    fn enter<'a, 'b>(&mut self, subgraph: &'a RefCell<&'b mut Subgraph<GOuter::Timestamp, TInner>>, communicator: C) -> Stream<(&'a RefCell<&'b mut Subgraph<GOuter::Timestamp, TInner>>, C), D> {

        let targets = OutputPort::<(GOuter::Timestamp, TInner), D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: ObserverHelper::new(targets.clone(), produced.clone()) };

        let scope_index = subgraph.borrow().index;
        let input_index = subgraph.borrow_mut().new_input(produced);

        self.connect_to(ScopeInput(scope_index, input_index), ingress);

        Stream::new(GraphInput(input_index), targets, (subgraph, communicator))
    }
}

pub trait LeaveSubgraphExt<GOuter: Graph, D: Data> {
    fn leave(&mut self, graph: &GOuter) -> Stream<GOuter, D>;
}

impl<'a, 'b, GOuter: Graph, TInner: Timestamp, D: Data> LeaveSubgraphExt<GOuter, D> for Stream<(&'a RefCell<&'b mut Subgraph<GOuter::Timestamp, TInner>>, GOuter::Communicator), D> {
    fn leave(&mut self, graph: &GOuter) -> Stream<GOuter, D> {

        let index = self.graph.0.borrow_mut().new_output();
        let targets = OutputPort::<GOuter::Timestamp, D>::new();

        self.connect_to(GraphOutput(index), EgressNub { targets: targets.clone(), phantom: PhantomData });

        Stream::new(ScopeOutput(self.graph.0.borrow_mut().index, index), targets, graph.clone())
    }
}


pub struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: ObserverHelper<OutputPort<(TOuter, TInner), TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Observer for IngressNub<TOuter, TInner, TData> {
    type Time = TOuter;
    type Data = TData;
    #[inline(always)] fn open(&mut self, time: &TOuter) -> () { self.targets.open(&(time.clone(), Default::default())); }
    #[inline(always)] fn show(&mut self, data: &TData) { self.targets.show(data); }
    #[inline(always)] fn give(&mut self, data:  TData) { self.targets.give(data); }
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
    #[inline(always)] fn show(&mut self, data: &TData) { self.targets.show(data); }
    #[inline(always)] fn give(&mut self, data:  TData) { self.targets.give(data); }
    #[inline(always)] fn shut(&mut self, time: &(TOuter, TInner)) { self.targets.shut(&time.0); }
}
