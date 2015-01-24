use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, PathSummary, Graph};
use progress::subgraph::Source;

use communication::Observer;
use communication::ProcessCommunicator;
use communication::Data;
use communication::channels::OutputPort;

pub struct Stream<T: Timestamp, S: PathSummary<T>, D:Data>
{
    pub name:       Source,                             // used to name the source in the host graph.
    pub ports:      OutputPort<T, D>,                   // used to register interest in the output.
    pub graph:      Box<Graph<T, S>>,                   // probably doesn't need to depend on T or S (make a new trait).
                                                        // (oops, it does; for graph.add_scope())
    pub allocator:  Rc<RefCell<ProcessCommunicator>>,   // for allocating communication channels
}

impl<T: Timestamp, S: PathSummary<T>, D:Data> Stream<T, S, D> {
    pub fn add_observer<O: Observer<Time=T, Data=D>>(&mut self, observer: O) {
        self.ports.shared.borrow_mut().push(Box::new(observer));
    }
}
