//! Tests for pipeline chain fusion.

use std::sync::{Arc, Mutex};
use timely::dataflow::operators::{ToStream, Inspect, Probe};
use timely::dataflow::operators::vec::Map;

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

/// Verifies that fusion is disabled when min_chain_length is 0.
#[test]
fn chain_fusion_disabled() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result2 = Arc::clone(&result);

    let config = timely::Config {
        communication: timely::CommunicationConfig::Thread,
        worker: timely::WorkerConfig::default().min_chain_length(0),
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
