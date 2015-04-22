use example::builder::Graph;

use progress::nested::subgraph::{Source, Target};

use communication::Observer;
use communication::Data;
use communication::output_port::Registrar;

use std::cell::RefCell;

pub struct Stream<'a, G: Graph+'a, D:Data> {
    pub name:   Source,                             // used to name the source in the host graph.
    pub ports:  Registrar<G::Timestamp, D>,         // used to register interest in the output.
                                                    // TODO : Make ports a reference like graph?
    pub graph:  &'a RefCell<G>,                     // graph builder for connecting edges, etc.
}

impl<'a, G: Graph+'a, D:Data> Stream<'a, G, D> {
    pub fn new(source: Source, output: Registrar<G::Timestamp, D>, graph: &'a RefCell<G>) -> Self {
        Stream {
            name: source,
            ports: output,
            graph: graph,
        }
    }

    pub fn clone_with<D2: Data>(&self, source: Source, targets: Registrar<G::Timestamp, D2>) -> Stream<'a, G, D2> {
        Stream::new(source, targets, self.graph)
    }

    pub fn connect_to<O: Observer<Time=G::Timestamp, Data=D>+'static>(&mut self, target: Target, observer: O) {
        self.graph.borrow_mut().connect(self.name, target);
        self.ports.add_observer(observer);
    }
}
