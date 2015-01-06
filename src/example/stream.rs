use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph};
use progress::subgraph::Source;

use communication::Observer;
use communication::ChannelAllocator;

pub struct Stream<T: Timestamp, S: PathSummary<T>, D:Copy+'static>
{
    pub name:       Source,                                     // used to name the source in the host graph.
    pub port:       Rc<RefCell<Vec<Box<Observer<(T, Vec<D>)>>>>>,    // used to register interest in the output.
    pub graph:      Box<Graph<T, S>>,                           // probably doesn't need to depend on T or S (make a new trait).
                                                                // (oops, it does; for graph.add_scope())
    pub allocator:  Rc<RefCell<ChannelAllocator>>,              // for allocating communication channels
}

impl<T: Timestamp, S: PathSummary<T>, D: Copy+'static> Stream<T, S, D>
{
    pub fn copy_with<D2: Copy+'static>(&self, name: Source, port: Rc<RefCell<Vec<Box<Observer<(T, Vec<D2>)>>>>>) -> Stream<T, S, D2>
    {
        Stream
        {
            name: name,
            port: port,
            graph: self.graph.as_box(),
            allocator: self.allocator.clone(),
        }
    }
}
