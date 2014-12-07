use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::subgraph::Source::{GraphInput, ScopeOutput};
use progress::subgraph::Target::{GraphOutput, ScopeInput};
use progress::subgraph::{Subgraph, Summary};
use progress::count_map::CountMap;

use example::stream::Stream;
use example::ports::{SourcePort, TargetPort};


pub trait GraphBuilder<T1:Timestamp, T2:Timestamp, S1:PathSummary<T1>, S2:PathSummary<T2>>
{
    // adds an input to self, from source, contained in graph.
    fn add_input<D:Copy+'static>(&mut self, source: &mut Stream<T1, S1, D>) ->
        Stream<(T1, T2), Summary<S1, S2>, D>;
    fn add_output_to_graph<D:Copy+'static>(&mut self, source: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                                      graph: Box<Graph<T1, S1>>) ->
        Stream<T1, S1, D>;
}

impl<TOuter, SOuter, TInner, SInner>
GraphBuilder<TOuter, TInner, SOuter, SInner>
for Rc<RefCell<Subgraph<TOuter, SOuter, TInner, SInner>>>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
{
    fn add_input<D: Copy+'static>(&mut self, source: &mut Stream<TOuter, SOuter, D>) ->
        Stream<(TOuter, TInner), Summary<SOuter, SInner>, D>
    {
        let mut borrow = self.borrow_mut();

        let messages = Rc::new(RefCell::new(Vec::new()));
        let ingress = Rc::new(RefCell::new(IngressNub
        {
            messages: messages.clone(),
            listeners: Vec::new(),
        }));

        let index = borrow.new_input(messages.clone());

        let send: Box<TargetPort<TOuter, D>> = box ingress.clone();

        source.graph.connect(source.name, ScopeInput(borrow.index, index));
        source.port.register_interest(send);

        return Stream { name: GraphInput(index), port: box ingress, graph: self.as_box() };
    }

    fn add_output_to_graph<D: Copy+'static>(&mut self, source: &mut Stream<(TOuter, TInner), Summary<SOuter, SInner>, D>,
                                                        graph: Box<Graph<TOuter, SOuter>>) -> Stream<TOuter, SOuter, D>
    {
        let mut borrow = self.borrow_mut();

        let index = borrow.new_output();

        let egress = Rc::new(RefCell::new(EgressNub
        {
            listeners: Vec::new(),
        }));

        borrow.connect(source.name, GraphOutput(index));
        source.port.register_interest(box egress.clone());

        return Stream{ name: ScopeOutput(borrow.index, index), port: box egress, graph: graph.as_box() };
    }
}


pub struct IngressNub<TOuter, TInner, Data>
{
    messages: Rc<RefCell<Vec<((TOuter, TInner), i64)>>>,
    listeners: Vec<Box<TargetPort<(TOuter, TInner), Data>>>,
}

impl<TOuter, TInner, Data>
SourcePort<(TOuter, TInner), Data>
for Rc<RefCell<IngressNub<TOuter, TInner, Data>>>
where TOuter: Timestamp, TInner: Timestamp, Data: Copy+'static
{
    fn register_interest(&mut self, target: Box<TargetPort<(TOuter, TInner), Data>>)
    {
        self.borrow_mut().listeners.push(target);
    }
}

impl<TOuter: Timestamp, TInner: Timestamp, Data: Copy+'static>
TargetPort<TOuter, Data>
for Rc<RefCell<IngressNub<TOuter, TInner, Data>>>
{
    fn deliver_data(&mut self, time: &TOuter, data: &Vec<Data>)
    {
        let new_time = (*time, Default::default());

        self.borrow_mut().messages.borrow_mut().update(new_time, 1);

        for listener in self.borrow_mut().listeners.iter_mut()
        {
            listener.deliver_data(&new_time, data);
        }
    }
}


pub struct EgressNub<TOuter, TInner, Data>
{
    listeners: Vec<Box<TargetPort<TOuter, Data>>>,
}

impl<TOuter, TInner, Data>
SourcePort<TOuter, Data>
for Rc<RefCell<EgressNub<TOuter, TInner, Data>>>
where TOuter: Timestamp, Data: Copy+'static
{
    fn register_interest(&mut self, target: Box<TargetPort<TOuter, Data>>)
    {
        self.borrow_mut().listeners.push(target);
    }
}

impl<TOuter, TInner, Data>
TargetPort<(TOuter, TInner), Data>
for Rc<RefCell<EgressNub<TOuter, TInner, Data>>>
where TOuter: Timestamp, TInner: Timestamp, Data: Copy+'static
{
    fn deliver_data(&mut self, time: &(TOuter, TInner), data: &Vec<Data>)
    {
        for listener in self.borrow_mut().listeners.iter_mut()
        {
            listener.deliver_data(&time.val0(), data);
        }
    }
}
