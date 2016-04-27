extern crate timely;
extern crate timely_communication;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;
use timely_communication::Allocate;

fn main() {
    // initializes and runs a timely dataflow computation
    timely::execute_from_args(std::env::args(), |computation| {
        // create a new input, exchange data, and inspect its output
        let (mut input, probe) = computation.scoped(move |builder| {
            let index = builder.index();
            let (input, stream) = builder.new_input();
            let probe = stream.exchange(|x| *x)
                              .inspect(move |x| println!("worker {}:\thello {}", index, x))
                              .probe().0;
            (input, probe)
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.le(&RootTimestamp::new(round)) {
                computation.step();
            }
        }
    }).unwrap();
}
