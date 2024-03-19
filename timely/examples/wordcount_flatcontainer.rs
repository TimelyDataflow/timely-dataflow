//! Wordcount based on flatcontainer.

#[cfg(feature = "bincode")]
use {
    std::collections::HashMap,
    timely::container::flatcontainer::{Containerized, FlatStack},
    timely::dataflow::channels::pact::{ExchangeCore, Pipeline},
    timely::dataflow::operators::core::InputHandle,
    timely::dataflow::operators::{Inspect, Operator, Probe},
    timely::dataflow::ProbeHandle,
};

#[cfg(feature = "bincode")]
fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input =
            <InputHandle<_, FlatStack<<(String, i64) as Containerized>::Region>>>::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .unary::<FlatStack<<(String, i64) as Containerized>::Region>, _, _, _>(
                    Pipeline,
                    "Split",
                    |_cap, _info| {
                        move |input, output| {
                            while let Some((time, data)) = input.next() {
                                let mut session = output.session(&time);
                                for (text, diff) in data.iter().flat_map(|(text, diff)| {
                                    text.split_whitespace().map(move |s| (s, diff))
                                }) {
                                    session.give((text, diff));
                                }
                            }
                        }
                    },
                )
                .unary_frontier::<FlatStack<<(String, i64) as Containerized>::Region>, _, _, _>(
                    ExchangeCore::new(|(s, _): &(&str, _)| s.len() as u64),
                    "WordCount",
                    |_capability, _info| {
                        let mut queues = HashMap::new();
                        let mut counts = HashMap::new();

                        move |input, output| {
                            while let Some((time, data)) = input.next() {
                                queues
                                    .entry(time.retain())
                                    .or_insert(Vec::new())
                                    .push(data.take());
                            }

                            for (key, val) in queues.iter_mut() {
                                if !input.frontier().less_equal(key.time()) {
                                    let mut session = output.session(key);
                                    for batch in val.drain(..) {
                                        for (word, diff) in batch.iter() {
                                            let entry =
                                                counts.entry(word.to_string()).or_insert(0i64);
                                            *entry += diff;
                                            session.give((word, *entry));
                                        }
                                    }
                                }
                            }

                            queues.retain(|_key, val| !val.is_empty());
                        }
                    },
                )
                .inspect(|x| println!("seen: {:?}", x))
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(("flat container", 1));
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}

#[cfg(not(feature = "bincode"))]
fn main() {
    eprintln!("Example requires feature bincode.");
}
