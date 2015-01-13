use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph};
use progress::subgraph::Source;

use communication::Observer;
use communication::ChannelAllocator;
use communication::Data;

pub struct Stream<T: Timestamp, S: PathSummary<T>, D:Data>
{
    pub name:       Source,                                 // used to name the source in the host graph.
    pub ports:      Rc<RefCell<Vec<Box<Observer<Time=T, Data=D>>>>>,  // used to register interest in the output.
    pub graph:      Box<Graph<T, S>>,                       // probably doesn't need to depend on T or S (make a new trait).
                                                            // (oops, it does; for graph.add_scope())
    pub allocator:  Rc<RefCell<ChannelAllocator>>,          // for allocating communication channels
}

impl<T: Timestamp, S: PathSummary<T>, D:Data> Stream<T, S, D>
{
    pub fn add_observer<O: Observer<Time=T, Data=D>>(&mut self, observer: O) {
        self.ports.borrow_mut().push(Box::new(observer));
    }

    // TODO : This causes rust 1.0 alpha (2015.01.12) to poop pants.
    // pub fn copy_with<D2: Data>(&self, name: Source, port: Rc<RefCell<Vec<Box<Observer<Time=T, Data=D2>>>>>) -> Stream<T, S, D2> {
    //     Stream {
    //         name: name,
    //         ports: port,
    //         graph: self.graph.as_box(),
    //         allocator: self.allocator.clone(),
    //     }
    // }
}
