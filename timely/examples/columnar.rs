//! Wordcount based on the `columnar` crate.

use std::collections::HashMap;

use columnar::Index;
use timely::Accountable;
use timely::container::CapacityContainerBuilder;
use timely::container::columnar::{ColumnarBuilder, ColumnarContainer};
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::{InspectCore, Operator, Probe};

// Creates `WordCountContainer` and `WordCountReference` structs,
// as well as various implementations relating them to `WordCount`.
#[derive(columnar::Columnar)]
struct WordCount {
    text: String,
    diff: i64,
}

fn main() {
    type InnerContainer = <WordCount as columnar::Columnar>::Container;
    type Container = ColumnarContainer<InnerContainer>;

    use columnar::Len;

    let config = timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(3),
        worker: timely::WorkerConfig::default(),
    };

    // initializes and runs a timely dataflow.
    timely::execute(config, |worker| {
        let mut input = <InputHandle<_, CapacityContainerBuilder<Container>>>::new();
        let probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .unary(Pipeline, "Split", |_cap, _info| {
                    move |input, output| {
                        input.for_each_time(|time, data| {
                            let mut session = output.session(&time);
                            for data in data {
                                for wordcount in
                                    data.borrow().into_index_iter().flat_map(|wordcount| {
                                        wordcount
                                            .text
                                            .split(|b| b.is_ascii_whitespace())
                                            .filter(|s| !s.is_empty())
                                            .map(move |text| WordCountReference {
                                                text,
                                                diff: wordcount.diff,
                                            })
                                    })
                                {
                                    session.give(wordcount);
                                }
                            }
                        });
                    }
                })
                .container::<Container>()
                .unary_frontier(
                    ExchangeCore::<ColumnarBuilder<InnerContainer>, _>::new_core(
                        |x: &WordCountReference<&[u8], &i64>| x.text.len() as u64,
                    ),
                    "WordCount",
                    |_capability, _info| {
                        let mut queues = HashMap::new();
                        let mut counts = HashMap::new();

                        move |(input, frontier), output| {
                            input.for_each_time(|time, data| {
                                queues
                                    .entry(time.retain(output.output_index()))
                                    .or_insert(Vec::new())
                                    .extend(data.map(std::mem::take));
                            });

                            for (key, val) in queues.iter_mut() {
                                if !frontier.less_equal(key.time()) {
                                    let mut session = output.session(key);
                                    for batch in val.drain(..) {
                                        for wordcount in batch.borrow().into_index_iter() {
                                            let total = if let Some(count) =
                                                counts.get_mut(wordcount.text)
                                            {
                                                *count += wordcount.diff;
                                                *count
                                            } else {
                                                counts.insert(
                                                    wordcount.text.to_vec(),
                                                    *wordcount.diff,
                                                );
                                                *wordcount.diff
                                            };
                                            session.give(WordCountReference {
                                                text: wordcount.text,
                                                diff: total,
                                            });
                                        }
                                    }
                                }
                            }

                            queues.retain(|_key, val| !val.is_empty());
                        }
                    },
                )
                .container::<Container>()
                .inspect_container(|x| match x {
                    Ok((time, data)) => {
                        println!("seen at: {:?}\t{:?} records", time, data.record_count());
                        for wc in data.borrow().into_index_iter() {
                            println!(
                                "  {}: {}",
                                std::str::from_utf8(wc.text).unwrap_or("<invalid utf8>"),
                                wc.diff
                            );
                        }
                    }
                    Err(frontier) => println!("frontier advanced to {:?}", frontier),
                })
                .probe_with(&probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(WordCountReference {
                text: "flat container",
                diff: 1,
            });
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}
