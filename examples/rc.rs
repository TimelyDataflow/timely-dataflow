extern crate abomonation;
extern crate timely;

use std::rc::Rc;
use timely::dataflow::operators::*;
use abomonation::Abomonation;

#[derive(Debug, Clone)]
pub struct Test {
    field: Rc<usize>,
}

impl Abomonation for Test {
    unsafe fn entomb(&self, _writer: &mut Vec<u8>) { panic!() }
    unsafe fn embalm(&mut self) { panic!() }
    unsafe fn exhume<'a,'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> { panic!()  }
}

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        // create a new input, exchange data, and inspect its output
        let index = worker.index();
        let (mut input, probe) = worker.dataflow(move |scope| {
            let (input, stream) = scope.new_input();
            let probe = stream//.exchange(|x| *x)
                           .inspect(move |x| println!("worker {}:\thello {:?}", index, x))
                           .probe();
            (input, probe)
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(Test { field: Rc::new(round) } );
            input.advance_to(round + 1);
            worker.step_while(|| probe.lt(input.time()));
        }
    }).unwrap();
}