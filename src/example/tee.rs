use std::rc::Rc;
use std::cell::RefCell;

use progress::subgraph::Source;
use progress::timestamp::Timestamp;
use example::ports::{SourcePort, TargetPort};

pub fn new_tee<T: Timestamp, D: Copy+'static>(_source: Source) -> (Box<SourcePort<T, D>>, Box<TargetPort<T, D>>)
{
    let vector: Vec<Box<TargetPort<T, D>>> = Vec::new();
    let listeners = Rc::new(RefCell::new(vector));

    (box listeners.clone(), box listeners)
}
