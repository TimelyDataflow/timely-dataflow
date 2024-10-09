//! Wordcount based on flatcontainer.

use {
    std::collections::HashMap,
    timely::{Container, container::CapacityContainerBuilder},
    timely::container::columnar::Columnar,
    timely::dataflow::channels::pact::{ExchangeCore, Pipeline},
    timely::dataflow::InputHandleCore,
    timely::dataflow::operators::{Inspect, Operator, Probe},
    timely::dataflow::ProbeHandle,
};

// Creates `WordCountContainer` and `WordCountReference` structs,
// as well as various implementations relating them to `WordCount`.
#[derive(Columnar)]
struct WordCount {
    text: String,
    diff: i64,
}

fn main() {

    type Container = Columnar<<WordCount as columnar::Columnar>::Container>;

    use columnar::Len;

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
                                for wordcount in data.iter().flat_map(|wordcount| {
                                    wordcount.text.split_whitespace().map(move |text| WordCountReference { text, diff: wordcount.diff })
                                }) {
                                    session.give(wordcount);
                                }
                            }
                        }
                    },
                )
                .container::<Container>()
                .unary_frontier(
                    ExchangeCore::new(|x: &WordCountReference<&str,&i64>| x.text.len() as u64),
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
                                        for wordcount in batch.iter() {
                                            let total =
                                            if let Some(count) = counts.get_mut(wordcount.text) {
                                                *count += wordcount.diff;
                                                *count
                                            }
                                            else {
                                                counts.insert(wordcount.text.to_string(), *wordcount.diff);
                                                *wordcount.diff
                                            };
                                            session.give(WordCountReference { text: wordcount.text, diff: total });
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
            input.send(WordCountReference { text: "flat container", diff: 1 });
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}
