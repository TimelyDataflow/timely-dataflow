use timely::dataflow::operators::*;
use timely::Config;

fn main() {
    timely::execute(Config::thread(), |worker| {
        let (mut input, mut cap) = worker.dataflow::<usize,_,_>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            stream.inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
            input
        });

        for round in 0..10 {
            input.activate().session(&cap).give(round);
            cap = cap.delayed(&(round + 1));
            worker.step();
        }
    }).unwrap();
}
