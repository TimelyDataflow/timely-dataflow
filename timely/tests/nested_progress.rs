//! Progress tracking through nested regions and an iterative scope.
//!
//! Records pass through several levels of regions, each holding them until the input frontier
//! passes their time, and then circulate through a loop for a fixed number of rounds. The test
//! checks that every round completes, that all records arrive exactly once, and that the
//! dataflow shuts down once its input closes.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::vec::{Filter, Map};
use timely::dataflow::operators::{CapabilitySet, Concat, ConnectLoop, Enter, Feedback, Input, Inspect, Leave, Probe};
use timely::dataflow::{InputHandle, ProbeHandle, Stream};
use timely::order::Product;
use timely::progress::Timestamp;

/// Holds records until the input frontier passes every time in their stamp.
fn hold<'s, T: Timestamp>(stream: Stream<'s, T, Vec<(u64, u64)>>) -> Stream<'s, T, Vec<(u64, u64)>> {
    stream.unary_frontier(Exchange::new(|x: &(u64, u64)| x.0), "Hold", |_cap, _info| {
        let mut stash: BTreeMap<Vec<T>, (CapabilitySet<T>, Vec<(u64, u64)>)> = BTreeMap::new();
        move |(input, frontier), output| {
            input.for_each_stamp(|cap, data| {
                let key = cap.stamp().elements().to_vec();
                let entry = stash.entry(key).or_insert_with(|| (cap.retain_stamp(0), Vec::new()));
                for d in data { entry.1.extend(d.drain(..)); }
            });
            let ready: Vec<Vec<T>> = stash.keys().filter(|s| !s.iter().any(|t| frontier.less_equal(t))).cloned().collect();
            for s in ready {
                let (caps, data) = stash.remove(&s).unwrap();
                output.session(&caps).give_iterator(data.into_iter());
            }
        }
    })
}

fn nest<'s, T: Timestamp>(stream: Stream<'s, T, Vec<(u64, u64)>>, depth: usize) -> Stream<'s, T, Vec<(u64, u64)>> {
    let outer = stream.scope();
    outer.region(|inner| {
        let s = hold(stream.enter(inner));
        let s = if depth > 1 {
            nest(s, depth - 1)
        }
        else {
            inner.iterative::<u32, _, _>(|it| {
                let (handle, cycle) = it.feedback(Product::new(Default::default(), 1));
                // The low two bits count down the remaining rounds.
                let body = hold(s.enter(it).map(|(k, v)| (k, v * 4 + 3)).concat(cycle));
                body.clone().filter(|x| x.1 % 4 != 0).map(|(k, v)| (k, v - 1)).connect_loop(handle);
                body.filter(|x| x.1 % 4 == 0).map(|(k, v)| (k, v / 4)).leave(inner)
            })
        };
        hold(s).leave(outer)
    })
}

fn run(config: timely::Config) {
    let results = timely::execute(config, |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();
        let probe = ProbeHandle::new();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = Rc::clone(&seen);
        worker.dataflow::<u64, _, _>(|scope| {
            nest(scope.input_from(&mut input).container::<Vec<(u64, u64)>>(), 4)
                .inspect(move |x| seen2.borrow_mut().push(*x))
                .probe_with(&probe);
        });
        for round in 0 .. 20u64 {
            if index == 0 {
                for i in 0 .. 3 { input.send((round * 3 + i, round * 3 + i)); }
            }
            input.advance_to(round + 1);
            let mut steps = 0;
            while probe.less_than(input.time()) {
                worker.step();
                steps += 1;
                assert!(steps < 100_000, "round {round} did not complete");
            }
        }
        drop(input);
        let mut steps = 0;
        while worker.step() {
            steps += 1;
            assert!(steps < 100_000, "dataflow did not shut down");
        }
        // The probe must observe the final frontier before the dataflow shuts down.
        assert!(probe.done());
        seen.take()
    }).unwrap().join();
    let mut all: Vec<(u64, u64)> = results.into_iter().flat_map(|r| r.unwrap()).collect();
    all.sort();
    let expected: Vec<(u64, u64)> = (0 .. 60).map(|x| (x, x)).collect();
    assert_eq!(all, expected);
}

#[test]
fn nested_progress_thread() { run(timely::Config::thread()); }

#[test]
fn nested_progress_process() { run(timely::Config::process(4)); }

/// A worker's own final update must reach operators before the dataflow completes.
fn probe_sees_final_frontier(config: timely::Config) {
    timely::execute(config, |worker| {
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            (input, stream.probe().0)
        });
        for round in 0 .. 5 {
            input.advance_to(round + 1);
            worker.step();
        }
        drop(input);
        while worker.step() { }
        assert!(probe.done());
    }).unwrap();
}

#[test]
fn probe_sees_final_frontier_thread() { probe_sees_final_frontier(timely::Config::thread()); }

#[test]
fn probe_sees_final_frontier_process() { probe_sees_final_frontier(timely::Config::process(2)); }
