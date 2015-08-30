extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;

fn main() {
    // initializes and runs a timely dataflow computation
    timely::execute_from_args(std::env::args(), |computation| {
        // create a new input, and inspect its output
        let mut input = computation.scoped(move |builder| {
            let (input, stream) = builder.new_input();
            stream.inspect(|x| println!("hello {}", x));
            input
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(format!("world: {:?}", round));
            input.advance_to(round + 1);
            computation.step();
        }
    });
}
