use std::rc::Rc;
use std::cell::RefCell;

use progress::Graph;
use progress::subgraph::{Source, Target};

use communication::Observer;
use communication::Communicator;
use communication::Data;
use communication::channels::OutputPort;

// pub trait StreamTrait {
//     type Data:         Data;
//     type Graph:        Graph;
//     type Communicator: Communicator;
// }

pub struct Stream<G: Graph, D:Data> {
    pub name:       Source,                         // used to name the source in the host graph.
    pub ports:      OutputPort<G::Timestamp, D>,    // used to register interest in the output.
    pub graph:      G,                              // graph builder for connecting edges, etc.
    pub allocator:  Rc<RefCell<CommunicatorEnum>>,      // for allocating communication channels
}

impl<G: Graph, D:Data> Stream<G, D> {
    pub fn add_observer<O: Observer<Time=G::Timestamp, Data=D>+'static>(&mut self, observer: O) {
        self.ports.shared.borrow_mut().push(Box::new(observer));
    }

    pub fn clone_with<D2: Data>(&self, source: Source, targets: OutputPort<G::Timestamp, D2>) -> Stream<G, D2> {
        Stream {
            name: source,
            ports: targets,
            graph: self.graph.clone(),
            allocator: self.allocator.clone(),
        }
    }

    pub fn connect_to<O: Observer<Time=G::Timestamp, Data=D>+'static>(&mut self, target: Target, observer: O) {
        self.graph.connect(self.name, target);
        self.add_observer(observer);
    }
}
