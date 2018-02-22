extern crate timely;

use timely::dataflow::operators::*;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
    let elements = std::env::args().nth(2).unwrap().parse::<u64>().unwrap();

    // initializes and runs a timely dataflow
    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        worker.dataflow(move |scope| {
            let (helper, cycle) = scope.loop_variable(iterations, 1);
            (0 .. elements)
                  .filter(move |&x| (x as usize) % peers == index)
                  .to_stream(scope)
                  .concat(&cycle)
                  .exchange(|&x| x)
                  .map_in_place(|x| *x += 1)
                  .connect_loop(helper);
        });
    }).unwrap();
}
