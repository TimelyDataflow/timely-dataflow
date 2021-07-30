extern crate timely;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let batch = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        let rounds = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let mut input = InputHandle::new();

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow(|scope|
            scope
                .input_from(&mut input)
                .exchange(|&x| x as u64)
                .probe()
        );

        let mut time = 0;

        let mut round_batch = 0;
        while round_batch < batch {
            let timer = std::time::Instant::now();

            for round in 0..rounds {
                for i in 0..round_batch {
                    input.send(i);
                }
                time += 1;
                input.advance_to(time);

                while probe.less_than(input.time()) {
                    worker.step();
                }
            }

            let volume = (rounds * batch) as f64;
            let elapsed = timer.elapsed();
            let seconds = elapsed.as_secs() as f64 + (f64::from(elapsed.subsec_nanos()) / 1000000000.0);

            if worker.index() == 0 {
                println!("{}\t{:?}\t{:?}", round_batch, timer.elapsed().as_secs_f64(), volume / seconds);
            }

            round_batch += std::cmp::max(worker.peers(), 64);
        }

    }).unwrap();
}
