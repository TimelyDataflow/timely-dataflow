extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::Summary::Local;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();

    // initializes and runs a timely dataflow computation
    timely::execute_from_args(std::env::args().skip(2), move |computation| {

        // create a new input, and inspect its output
        let mut input = computation.scoped(move |builder| {
            let (input, stream) = builder.new_input();
            let (helper, cycle) = builder.loop_variable(RootTimestamp::new(iterations), Local(1));
            stream.concat(&cycle).exchange(|&x| x).map(|x| x + 1).connect_loop(helper);
            input
        });

        // introduce data and watch!
        for round in 0..1 {
            input.send(0);
            input.advance_to(round + 1);
            computation.step();
        }
    });
}
