use timely::dataflow::operators::{core::UnorderedInput, Inspect};
use timely::Config;

fn main() {
    timely::execute(Config::thread(), |worker| {
        let (mut input, mut cap) = worker.dataflow::<usize,_,_>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            stream.container::<Vec<_>>().inspect_core(|e| if let Ok((s, x)) = e { println!("{:?} -> {:?}", s, x) });
            input
        });

        for round in 0..10 {
            input.activate().session(&cap).give(round);
            cap = cap.delayed(&(round + 1));
            worker.step();
        }
    }).unwrap();
}
