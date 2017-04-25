extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        // create a new input, exchange data, and inspect its output
        let (mut input, probe) = worker.dataflow(move |scope| {
            let index = scope.index();
            let (input, stream) = scope.new_input();
            let probe = stream.exchange(|x| *x)
                              .inspect(move |x| println!("worker {}:\thello {}", index, x))
                              .probe();
            (input, probe)
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(round);
            input.advance_to(round + 1);
            worker.step_while(|| probe.lt(input.time()));
        }
    }).unwrap();
}
