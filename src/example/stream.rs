use std::rc::Rc;
use std::cell::RefCell;

use progress::Graph;
use progress::subgraph::Source;

use communication::Observer;
use communication::Communicator;
use communication::Data;
use communication::channels::OutputPort;

pub struct Stream<G: Graph, D:Data> {
    pub name:       Source,                         // used to name the source in the host graph.
    pub ports:      OutputPort<G::Timestamp, D>,    // used to register interest in the output.
    pub graph:      G,                              // probably doesn't need to depend on T or S (make a new trait).
                                                    // (oops, it does; for graph.add_scope())
    pub allocator:  Rc<RefCell<Communicator>>,      // for allocating communication channels
}

impl<G: Graph, D:Data> Stream<G, D> {
    pub fn add_observer<O: Observer<Time=G::Timestamp, Data=D>+'static>(&mut self, observer: O) {
        self.ports.shared.borrow_mut().push(Box::new(observer));
    }
}
