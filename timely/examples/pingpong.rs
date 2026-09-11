use timely::dataflow::operators::{ToStream, Exchange, Feedback, Concat, ConnectLoop, vec::{Map, Filter}};

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
    let elements = std::env::args().nth(2).unwrap().parse::<u64>().unwrap();

    // initializes and runs a timely dataflow
    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        worker.dataflow::<u64,_,_>(move |scope| {
            let (helper, cycle) = scope.feedback(1);
            // Each record counts the rounds it has made; all start at zero, and each
            // makes exactly `iterations` trips around the loop.
            (0 .. elements)
                  .filter(move |&i| (i as usize) % peers == index)
                  .map(|_| 0u64)
                  .to_stream(scope)
                  .concat(cycle)
                  .exchange(|&x| x)
                  .map_in_place(|x| *x += 1)
                  .filter(move |&x| x <= iterations)
                  .connect_loop(helper);
        });
    }).unwrap();
}
