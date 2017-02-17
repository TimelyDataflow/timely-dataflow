extern crate abomonation;
extern crate timely;

use std::rc::Rc;
use timely::dataflow::*;
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
   // initializes and runs a timely dataflow computation
   timely::execute_from_args(std::env::args(), |computation| {
       // create a new input, exchange data, and inspect its output
       let (mut input, probe) = computation.scoped(move |builder| {
           let index = builder.index();
           let (input, stream) = builder.new_input();
           let probe = stream//.exchange(|x| *x)
                           .inspect(move |x| println!("worker {}:\thello {:?}", index, x))
                           .probe().0;
           (input, probe)
       });

       // introduce data and watch!
       for round in 0..10 {
           input.send(Test { field: Rc::new(round) } );
           input.advance_to(round + 1);
           computation.step_while(|| probe.lt(input.time()));
       }
   }).unwrap();
}