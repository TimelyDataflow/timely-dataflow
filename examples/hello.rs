extern crate timely;

use timely::example_shared::*;
use timely::example_shared::operators::*;

fn main() {

    // initializes and runs a timely dataflow computation
    timely::initialize(std::env::args(), |communicator| {

        // define a new computation using the communicator
        let mut computation = GraphRoot::new(communicator);

        // create a new input, and inspect its output
        let mut input = computation.subcomputation(move |builder| {
            let (input, stream) = builder.new_input();
            stream.inspect(move |x| println!("hello {:?}", x));
            input
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send_at(round, round..round+1);
            input.advance_to(round + 1);
            computation.step();
        }

        // seal the input
        input.close();

        // finish off any remaining work
        while computation.step() { }
    });
}
