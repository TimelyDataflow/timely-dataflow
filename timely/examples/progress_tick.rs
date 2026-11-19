//! Measures the cost of advancing input frontiers through dataflows that carry no data.
//!
//! Usage: progress_tick [dataflows] [regions] [operators] [ticks] [rounds]
//!
//! Builds `dataflows` dataflows, each a chain of `regions` regions that each hold a chain of
//! `operators` frontier-observing operators. Each tick advances every input by one and steps
//! the worker until every probe has caught up.

use std::time::Instant;

use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    let mut args = std::env::args().skip(1).map(|x| x.parse::<usize>().unwrap());
    let dataflows = args.next().unwrap_or(100);
    let regions = args.next().unwrap_or(5);
    let operators = args.next().unwrap_or(10);
    let ticks = args.next().unwrap_or(200);
    let rounds = args.next().unwrap_or(5);

    timely::execute_directly(move |worker| {
        let mut inputs = Vec::new();
        let mut probe = ProbeHandle::new();
        for _ in 0..dataflows {
            let mut input: InputHandle<u64, _> = InputHandle::new();
            worker.dataflow(|scope| {
                let mut stream: timely::dataflow::StreamVec<_, u64> = scope.input_from(&mut input);
                for _ in 0..regions {
                    stream = scope.clone().region(|inner| {
                        use timely::dataflow::operators::{Enter, Leave};
                        let mut s = stream.enter(inner);
                        for _ in 0..operators {
                            s = s.unary_frontier(Pipeline, "Frontier", |_, _| {
                                move |(input, _frontier), output| {
                                    input.for_each(|time, data| {
                                        output.session(&time).give_container(data);
                                    });
                                }
                            });
                        }
                        s.leave(scope)
                    });
                }
                stream.probe_with(&mut probe);
            });
            inputs.push(input);
        }

        let mut time = 0u64;
        let mut tick = |worker: &mut timely::worker::Worker| {
            time += 1;
            for input in inputs.iter_mut() { input.advance_to(time); }
            while probe.less_than(&time) { worker.step(); }
        };
        for _ in 0..20 { tick(worker); }

        let ops = dataflows * regions * operators;
        let mut best = f64::INFINITY;
        for _ in 0..rounds {
            let start = Instant::now();
            for _ in 0..ticks { tick(worker); }
            let ns = start.elapsed().as_nanos() as f64 / (ticks * ops) as f64;
            best = best.min(ns);
            println!("{ns:.1} ns/operator/tick");
        }
        println!("best {best:.1} ns/operator/tick ({ops} operators)");
    });
}
