//! Wordcount based on flatcontainer.

use {
    std::collections::HashMap,
    timely::container::CapacityContainerBuilder,
    timely::container::flatcontainer::{RegionPreference, FlatStack},
    timely::dataflow::channels::pact::{ExchangeCore, Pipeline},
    timely::dataflow::InputHandleCore,
    timely::dataflow::operators::{Inspect, Operator, Probe},
    timely::dataflow::ProbeHandle,
};

fn main() {

    type Container = FlatStack<<(String, i64) as RegionPreference>::Region>;

    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .unary(
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
                .container::<Container>()
                .unary_frontier(
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
                                            let total =
                                            if let Some(count) = counts.get_mut(word) {
                                                *count += diff;
                                                *count
                                            }
                                            else {
                                                counts.insert(word.to_string(), diff);
                                                diff
                                            };
                                            session.give((word, total));
                                        }
                                    }
                                }
                            }

                            queues.retain(|_key, val| !val.is_empty());
                        }
                    },
                )
                .container::<Container>()
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
