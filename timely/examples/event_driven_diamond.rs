use timely::dataflow::operators::{Input, Concat, Probe};
use timely::dataflow::operators::vec::{Map, Filter};

fn main() {
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
        let diamonds = positional[1].parse::<usize>().unwrap();
        let record = positional.get(2).map(|s| s.as_str()) == Some("record");
        let rounds: usize = positional.get(3).map(|s| s.parse().unwrap()).unwrap_or(usize::MAX);

        let mut inputs = Vec::new();
        let mut probes = Vec::new();

        // Each dataflow builds a chain of diamond patterns:
        //   input -> map (left) + map (right) -> concat -> ... -> probe
        // Each diamond has 3 operators (map, map, concat).
        // The clone/branch doesn't create an operator — it reuses the stream's Tee.
        for _dataflow in 0..dataflows {
            worker.dataflow(|scope| {
                let (input, mut stream) = scope.new_input();
                for _diamond in 0..diamonds {
                    let left = stream.clone().map(|x: ()| x);
                    let right = stream.filter(|_| false).map(|x: ()| x);
                    stream = left.concat(right).container::<Vec<_>>();
                }
                let (probe, _stream) = stream.probe();
                inputs.push(input);
                probes.push(probe);
            });
        }

        println!("{:?}\tdataflows built ({} x {} diamonds)", timer.elapsed(), dataflows, diamonds);

        for round in 0..rounds {
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
            println!("{:?}\tround {} complete in {} steps", timer.elapsed(), round, steps);
        }

    }).unwrap();
}
