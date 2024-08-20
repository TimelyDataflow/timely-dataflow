extern crate timely;

use std::rc::Rc;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Inspect, Probe};

#[derive(Debug, Clone)]
pub struct Test {
    _field: Rc<usize>,
}

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        // create a new input, exchange data, and inspect its output
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 //.exchange(|x| *x) // <-- cannot exchange this; Rc is not Send.
                 .inspect(move |x| println!("worker {}:\thello {:?}", index, x))
                 .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(Test { _field: Rc::new(round) } );
            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(input.time()));
        }
    }).unwrap();
}
