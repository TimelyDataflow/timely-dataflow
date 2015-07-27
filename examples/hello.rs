extern crate timely;

use timely::construction::*;
use timely::construction::operators::*;

fn main() {

    // initializes and runs a timely dataflow computation
    timely::execute(std::env::args(), |computation| {

        // create a new input, and inspect its output
        let mut input = computation.subcomputation(move |builder| {
            let (input, stream) = builder.new_input();
            stream.inspect(|x| println!("hello: {:?}", x));
            input
        });

        // introduce data and watch!
        for round in 0..10 {
            input.give(round);
            input.advance_to(round + 1);
            computation.step();
        }

        // seal the input
        input.close();

        // finish off any remaining work
        while computation.step() { }

    });
}
