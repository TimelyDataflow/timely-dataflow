//! Tests for pipeline group fusion.

use std::sync::{Arc, Mutex};
use timely::dataflow::operators::{ToStream, Concat, Inspect, Probe, Feedback, ConnectLoop};
use timely::dataflow::operators::vec::{Map, Filter, Partition};

/// Verifies that a chain of map operators produces correct output.
#[test]
fn chain_fusion_correctness() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        worker.dataflow::<usize, _, _>(|scope| {
            (0..10u64)
                .to_stream(scope)
                .map(|x| x + 1)
                .map(|x| x * 2)
                .map(|x| x + 10)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let expected: Vec<u64> = (0..10).map(|x| (x + 1) * 2 + 10).collect();
    assert_eq!(got, expected);
}

/// Verifies that a longer chain of maps (5 operators) produces correct output.
#[test]
fn chain_fusion_long_chain() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        worker.dataflow::<usize, _, _>(|scope| {
            (0..5u64)
                .to_stream(scope)
                .map(|x| x + 1)
                .map(|x| x * 2)
                .map(|x| x + 3)
                .map(|x| x * 4)
                .map(|x| x + 5)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let expected: Vec<u64> = (0..5).map(|x| ((x + 1) * 2 + 3) * 4 + 5).collect();
    assert_eq!(got, expected);
}

/// Verifies that fusion works with probe (which tests that the dataflow completes).
#[test]
fn chain_fusion_with_probe() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let probe = worker.dataflow::<usize, _, _>(|scope| {
            (0..100u64)
                .to_stream(scope)
                .map(|x| x + 1)
                .map(|x| x * 2)
                .map(|x| x + 10)
                .probe()
                .0
        });

        worker.step_while(|| probe.less_than(&usize::MAX));
    }).unwrap();
}

/// Verifies that fusion is disabled when fuse_chain_length is 0.
#[test]
fn chain_fusion_disabled() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    let config = timely::Config {
        communication: timely::CommunicationConfig::Thread,
        worker: timely::WorkerConfig::default().fuse_chain_length(0),
    };

    timely::execute(config, move |worker| {
        let result3 = Arc::clone(&result2);
        worker.dataflow::<usize, _, _>(|scope| {
            (0..10u64)
                .to_stream(scope)
                .map(|x| x + 1)
                .map(|x| x * 2)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let expected: Vec<u64> = (0..10).map(|x| (x + 1) * 2).collect();
    assert_eq!(got, expected);
}

/// Verifies that fusion works with notify=true operators (inspect uses unary_frontier).
/// This test drives multiple rounds to exercise frontier propagation within the fused chain.
#[test]
fn chain_fusion_notify_operator() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        let (mut input, probe) = worker.dataflow::<usize, _, _>(|scope| {
            use timely::dataflow::operators::Input;
            let (input, stream) = scope.new_input();
            let probe = stream
                .map(|x: u64| x + 1)
                .map(|x| x * 2)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                })
                .probe()
                .0;
            (input, probe)
        });

        for round in 0..5usize {
            input.send(round as u64);
            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(&(round + 1)));
        }
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let expected: Vec<u64> = (0..5).map(|x: u64| (x + 1) * 2).collect();
    assert_eq!(got, expected);
}

/// Verifies that fusion works with a unary_notify operator that buffers data
/// and emits on frontier notification. This exercises frontier propagation within
/// the fused chain.
#[test]
fn chain_fusion_unary_notify() {
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::generic::operator::Operator;

    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        let (mut input, probe) = worker.dataflow::<usize, _, _>(|scope| {
            use timely::dataflow::operators::Input;
            let (input, stream) = scope.new_input();
            let probe = stream
                .map(|x: u64| x + 1)
                // A unary_notify operator that buffers data and emits on notification.
                // This is a notify=true operator with 1 input and 1 output.
                .unary_notify(Pipeline, "Buffer", vec![], {
                    let mut stash: std::collections::HashMap<usize, Vec<u64>> = std::collections::HashMap::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            stash.entry(time.time().clone())
                                .or_insert_with(Vec::new)
                                .extend(data.drain(..));
                            notificator.notify_at(time.retain(0));
                        });
                        notificator.for_each(|time, _count, _notify| {
                            if let Some(data) = stash.remove(time.time()) {
                                let mut session = output.session(&time);
                                for datum in data {
                                    session.give(datum);
                                }
                            }
                        });
                    }
                })
                .map(|x: u64| x * 10)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                })
                .probe()
                .0;
            (input, probe)
        });

        for round in 0..5usize {
            input.send(round as u64);
            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(&(round + 1)));
        }
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    // Each value: (x + 1) buffered by unary_notify, then * 10
    let expected: Vec<u64> = (0..5).map(|x: u64| (x + 1) * 10).collect();
    assert_eq!(got, expected);
}

/// Verifies that flat_map (single in/out, pipeline, notify=false) is also fused.
#[test]
fn chain_fusion_flat_map() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        worker.dataflow::<usize, _, _>(|scope| {
            (0..5u64)
                .to_stream(scope)
                .map(|x| x + 1)
                .flat_map(|x| vec![x, x * 10])
                .map(|x| x + 100)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let mut expected: Vec<u64> = (0..5u64)
        .map(|x| x + 1)
        .flat_map(|x| vec![x, x * 10])
        .map(|x| x + 100)
        .collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// Diamond pattern: stream -> map (left) + map (right) -> concat -> inspect.
/// All operators are fusible (!notify, pipeline, identity summary).
#[test]
fn group_fusion_diamond() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        worker.dataflow::<usize, _, _>(|scope| {
            let stream = (0..10u64).to_stream(scope);
            let left = stream.clone().map(|x| x + 1);
            let right = stream.map(|x| x + 100);
            left.concat(right)
                .map(|x| x * 2)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let mut expected: Vec<u64> = (0..10u64)
        .flat_map(|x| vec![(x + 1) * 2, (x + 100) * 2])
        .collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// Diamond with probe: verifies dataflow completion with DAG fusion.
#[test]
fn group_fusion_diamond_with_probe() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        let (mut input, probe) = worker.dataflow::<usize, _, _>(|scope| {
            use timely::dataflow::operators::Input;
            let (input, stream) = scope.new_input();
            let left = stream.clone().map(|x: u64| x + 1);
            let right = stream.map(|x: u64| x + 100);
            let probe = left.concat(right)
                .map(|x| x * 2)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                })
                .probe()
                .0;
            (input, probe)
        });

        for round in 0..5usize {
            input.send(round as u64);
            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(&(round + 1)));
        }
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let mut expected: Vec<u64> = (0..5u64)
        .flat_map(|x| vec![(x + 1) * 2, (x + 100) * 2])
        .collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// Multi-input merge: two independent input streams -> concat -> map.
#[test]
fn group_fusion_multi_input_merge() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let result3 = Arc::clone(&result2);
        worker.dataflow::<usize, _, _>(|scope| {
            let s1 = (0..5u64).to_stream(scope).map(|x| x + 1);
            let s2 = (10..15u64).to_stream(scope).map(|x| x + 1);
            s1.concat(s2)
                .map(|x| x * 3)
                .inspect(move |x| {
                    result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got = result.lock().unwrap().clone();
    got.sort();
    let mut expected: Vec<u64> = (0..5u64).map(|x| (x + 1) * 3)
        .chain((10..15u64).map(|x| (x + 1) * 3))
        .collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// Branch without merge: map -> (map + map) with two separate outputs consumed by inspect.
/// The two branches are not merged back, testing fan-out group outputs.
#[test]
fn group_fusion_branch() {
    let left_result = Arc::new(Mutex::new(Vec::new()));
    let right_result = Arc::new(Mutex::new(Vec::new()));
    let left_result2 = Arc::clone(&left_result);
    let right_result2 = Arc::clone(&right_result);

    timely::execute_from_args(std::env::args(), move |worker| {
        let left_result3 = Arc::clone(&left_result2);
        let right_result3 = Arc::clone(&right_result2);
        worker.dataflow::<usize, _, _>(|scope| {
            let stream = (0..5u64).to_stream(scope).map(|x| x + 1);
            // Two branches from the same map output.
            stream.clone().map(|x| x * 2)
                .inspect(move |x| {
                    left_result3.lock().unwrap().push(*x);
                });
            stream.map(|x| x * 3)
                .inspect(move |x| {
                    right_result3.lock().unwrap().push(*x);
                });
        });
    }).unwrap();

    let mut got_left = left_result.lock().unwrap().clone();
    got_left.sort();
    let expected_left: Vec<u64> = (0..5).map(|x| (x + 1) * 2).collect();
    assert_eq!(got_left, expected_left);

    let mut got_right = right_result.lock().unwrap().clone();
    got_right.sort();
    let expected_right: Vec<u64> = (0..5).map(|x| (x + 1) * 3).collect();
    assert_eq!(got_right, expected_right);
}

/// Collatz mutual recursion with feedback loops.
/// This exercises DAG fusion with external feedback edges.
#[test]
fn group_fusion_collatz_mutual_recursion() {
    let config = timely::Config {
        communication: timely::CommunicationConfig::Thread,
        worker: timely::WorkerConfig::default(),
    };

    timely::execute(config, |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let (handle0, stream0) = scope.feedback(1);
            let (handle1, stream1) = scope.feedback(1);

            let results0 = stream0.map(|x: u64| x / 2).filter(|x| *x != 1);
            let results1 = stream1.map(|x: u64| 3 * x + 1);

            let mut parts =
                (1u64..10)
                    .to_stream(scope)
                    .concat(results0)
                    .concat(results1)
                    .inspect(|_x| {})
                    .partition(2, |x| (x % 2, x));

            parts.pop().unwrap().connect_loop(handle1);
            parts.pop().unwrap().connect_loop(handle0);
        });
    }).unwrap();
}

/// Repeated diamond chain with probe: tests larger fused groups.
#[test]
fn group_fusion_repeated_diamonds_with_probe() {
    use timely::dataflow::operators::Input;

    timely::execute_from_args(std::env::args(), move |worker| {
        let (mut input, probe) = worker.dataflow(|scope| {
            let (input, mut stream) = scope.new_input();
            for _diamond in 0..15 {
                let left = stream.clone().map(|x: u64| x);
                let right = stream.map(|x: u64| x);
                stream = left.concat(right).container::<Vec<_>>();
            }
            let (probe, _stream) = stream.probe();
            (input, probe)
        });

        for round in 0..5usize {
            input.send(0u64);
            input.advance_to(round);
            while probe.less_than(&round) {
                worker.step();
            }
        }
    }).unwrap();
}
