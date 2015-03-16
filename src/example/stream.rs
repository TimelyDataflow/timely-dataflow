use std::rc::Rc;
use std::cell::RefCell;

use progress::Graph;
use progress::subgraph::{Source, Target};

use communication::Observer;
use communication::Communicator;
use communication::Data;
use communication::channels::OutputPort;

pub struct Stream<G: Graph, D:Data, C: Communicator> {
    pub name:       Source,                         // used to name the source in the host graph.
    pub ports:      OutputPort<G::Timestamp, D>,    // used to register interest in the output.
    pub graph:      G,                              // graph builder for connecting edges, etc.
    pub allocator:  Rc<RefCell<C>>,                 // for allocating communication channels
}

impl<G: Graph, D:Data, C: Communicator> Stream<G, D, C> {
    pub fn clone_with<D2: Data>(&self, source: Source, targets: OutputPort<G::Timestamp, D2>) -> Stream<G, D2, C> {
        Stream {
            name: source,
            ports: targets,
            graph: self.graph.clone(),
            allocator: self.allocator.clone(),
        }
    }

    pub fn connect_to<O: Observer<Time=G::Timestamp, Data=D>+'static>(&mut self, target: Target, observer: O) {
        self.graph.connect(self.name, target);
        self.ports.add_observer(observer);
    }
}
