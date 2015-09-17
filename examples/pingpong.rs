extern crate timely;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();

    // initializes and runs a timely dataflow computation
    timely::execute_from_args(std::env::args().skip(2), move |computation| {
        let index = computation.index();
        computation.scoped(move |builder| {
            let (helper, cycle) = builder.loop_variable(iterations, 1);
            (0..1).take(if index == 0 { 1 } else { 0 })
                  .to_stream(builder)
                  .concat(&cycle)
                  .exchange(|&x| x)
                  .map_in_place(|x| *x += 1)
                  .connect_loop(helper);
        });
    });
}
