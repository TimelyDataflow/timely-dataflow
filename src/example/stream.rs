use progress::Graph;
use progress::nested::subgraph::{Source, Target};

use communication::Observer;
use communication::Data;
use communication::channels::OutputPort;

pub struct Stream<G: Graph, D:Data> {
    name:           Source,                         // used to name the source in the host graph.
    ports:          OutputPort<G::Timestamp, D>,    // used to register interest in the output.
    pub graph:      G,                              // graph builder for connecting edges, etc.
}

impl<G: Graph, D:Data> Stream<G, D> {
    pub fn new(source: Source, output: OutputPort<G::Timestamp, D>, graph: G) -> Self {
        Stream {
            name: source,
            ports: output,
            graph: graph,
        }
    }

    pub fn clone_with<D2: Data>(&self, source: Source, targets: OutputPort<G::Timestamp, D2>) -> Stream<G, D2> {
        Stream::new(source, targets, self.graph.clone())
    }

    pub fn connect_to<O: Observer<Time=G::Timestamp, Data=D>+'static>(&mut self, target: Target, observer: O) {
        self.graph.connect(self.name, target);
        self.ports.add_observer(observer);
    }
}
