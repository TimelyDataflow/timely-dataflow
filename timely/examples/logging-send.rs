use std::time::Duration;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Probe};
use timely::logging::{TimelyEventBuilder, TimelyProgressEventBuilder, TimelySummaryEventBuilder};
use timely::container::CapacityContainerBuilder;
use timely::progress::reachability::logging::TrackerEventBuilder;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let batch = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        let rounds = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Register timely worker logging.
        worker.log_register().insert::<TimelyEventBuilder,_>("timely", |time, data|
            if let Some(data) = data {
                data.iter().for_each(|x| println!("LOG1: {:?}", x))
            }
            else {
                println!("LOG1: Flush {time:?}");
            }
        );

        // Register timely progress logging.
        // Less generally useful: intended for debugging advanced custom operators or timely
        // internals.
        worker.log_register().insert::<TimelyProgressEventBuilder<usize>,_>("timely/progress/usize", |time, data|
            if let Some(data) = data {
                data.iter().for_each(|x| {
                    println!("PROGRESS: {:?}", x);
                    let (_, ev) = x;
                    print!("PROGRESS: TYPED MESSAGES: ");
                    for (n, p, t, d) in ev.messages.iter() {
                        print!("{:?}, ", (n, p, t, d));
                    }
                    println!();
                    print!("PROGRESS: TYPED INTERNAL: ");
                    for (n, p, t, d) in ev.internal.iter() {
                        print!("{:?}, ", (n, p, t, d));
                    }
                    println!();
                })
            }
            else {
                println!("PROGRESS: Flush {time:?}");
            }
        );

        worker.log_register().insert::<TrackerEventBuilder<usize>,_>("timely/reachability/usize", |time, data|
            if let Some(data) = data {
                data.iter().for_each(|x| {
                    println!("REACHABILITY: {:?}", x);
                })
            }
            else {
                println!("REACHABILITY: Flush {time:?}");
            }
        );

        worker.log_register().insert::<TimelySummaryEventBuilder<usize>,_>("timely/summary/usize", |time, data|
            if let Some(data) = data {
                data.iter().for_each(|(_, x)| {
                    println!("SUMMARY: {:?}", x);
                })
            }
            else {
                println!("SUMMARY: Flush {time:?}");
            }
        );

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|&x| x as u64)
                .probe_with(&mut probe);
        });

        // Register timely worker logging.
        worker.log_register().insert::<TimelyEventBuilder,_>("timely", |time, data|
            if let Some(data) = data {
                data.iter().for_each(|x| println!("LOG2: {:?}", x))
            }
            else {
                println!("LOG2: Flush {time:?}");
            }
        );

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|&x| x as u64)
                .probe_with(&mut probe);
        });

        // Register user-level logging.
        type MyBuilder = CapacityContainerBuilder<Vec<(Duration, ())>>;
        worker.log_register().insert::<MyBuilder,_>("input", |time, data|
            if let Some(data) = data {
                for element in data.iter() {
                    println!("Round tick at: {:?}", element.0);
                }
            }
            else {
                println!("Round flush at: {time:?}");
            }
        );

        let input_logger = worker.log_register().get::<MyBuilder>("input").expect("Input logger absent");

        let timer = std::time::Instant::now();

        for round in 0 .. rounds {

            for i in 0 .. batch {
                input.send(i);
            }
            input.advance_to(round);
            input_logger.log(());

            while probe.less_than(input.time()) {
                worker.step();
            }

        }

        let volume = (rounds * batch) as f64;
        let elapsed = timer.elapsed();
        let seconds = elapsed.as_secs() as f64 + (f64::from(elapsed.subsec_nanos())/1000000000.0);

        println!("{:?}\tworker {} complete; rate: {:?}", timer.elapsed(), worker.index(), volume / seconds);

    }).unwrap();
}
