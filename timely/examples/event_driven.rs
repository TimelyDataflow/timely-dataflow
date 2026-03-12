use timely::dataflow::Scope;
use timely::dataflow::operators::{Input, Probe, Enter, Leave};
use timely::dataflow::operators::vec::Map;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let timer = std::time::Instant::now();

        // Collect positional arguments, skipping flags consumed by timely (-w, -n, -p, -h).
        let positional: Vec<String> = {
            let mut pos = Vec::new();
            let mut args = std::env::args();
            args.next(); // skip binary name
            while let Some(arg) = args.next() {
                if arg.starts_with('-') {
                    args.next(); // skip flag value
                } else {
                    pos.push(arg);
                }
            }
            pos
        };

        let dataflows = positional[0].parse::<usize>().unwrap();
        let length = positional[1].parse::<usize>().unwrap();
        let record = positional.get(2).map(|s| s.as_str()) == Some("record");
        let rounds: usize = positional.get(3).map(|s| s.parse().unwrap()).unwrap_or(usize::MAX);

        let mut inputs = Vec::new();
        let mut probes = Vec::new();

        // create a new input, exchange data, and inspect its output
        for _dataflow in 0 .. dataflows {
            worker.dataflow(|scope| {
                let (input, stream) = scope.new_input();
                let stream = scope.region(|inner| {
                    let mut stream = stream.enter(inner);
                    for _step in 0 .. length {
                        stream = stream.map(|x: ()| x);
                    }
                    stream.leave()
                });
                let (probe, _stream) = stream.probe();
                inputs.push(input);
                probes.push(probe);
            });
        }

        println!("{:?}\tdataflows built ({} x {})", timer.elapsed(), dataflows, length);

        for round in 0 .. rounds {
            let dataflow = round % dataflows;
            if record {
                inputs[dataflow].send(());
            }
            inputs[dataflow].advance_to(round);
            let mut steps = 0;
            while probes[dataflow].less_than(&round) {
                worker.step();
                steps += 1;
            }
            if round % 1000 == 0 { println!("{:?}\tround {} complete in {} steps", timer.elapsed(), round, steps); }
        }

    }).unwrap();
}
