//! Deterministic simulation tests: run a multi-worker barrier under seeded random
//! schedules of worker steps and message deliveries, asserting progress-tracking
//! safety (no round notified twice, rounds in order) on every explored schedule.

use std::rc::Rc;
use std::cell::RefCell;

use timely::simulate::{Decision, Simulation};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;
use timely::container::CapacityContainerBuilder;

const ROUNDS: usize = 25;

/// A tiny deterministic RNG (SplitMix64); the harness stays free of external deps.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % (bound as u64)) as usize
    }
}

/// Runs a barrier to completion under a seeded random schedule prefix followed by a
/// fair drain. Returns the sequence of (worker, round) notifications, in the order
/// they occurred across all workers (meaningful: everything is on one thread).
fn barrier_run(peers: usize, seed: u64, prefix: usize) -> Vec<(usize, usize)> {

    let mut sim = Simulation::new(peers);
    let log = Rc::new(RefCell::new(Vec::new()));

    for index in 0 .. peers {
        let log = Rc::clone(&log);
        sim.worker_mut(index).dataflow(move |scope| {
            let (handle, stream) = scope.feedback::<Vec<usize>>(1);
            stream.unary_notify::<CapacityContainerBuilder<_>, _, _>(
                Pipeline,
                "Barrier",
                vec![0],
                move |_, _, notificator| {
                    let mut count = 0;
                    while let Some((cap, _cnt)) = notificator.next() {
                        count += 1;
                        let round = *cap.time();
                        log.borrow_mut().push((index, round));
                        if round + 1 < ROUNDS {
                            notificator.notify_at(cap.delayed(&(round + 1)));
                        }
                    }
                    // Progress safety: at most one round may be notified per scheduling.
                    assert!(count <= 1);
                }
            )
            .connect_loop(handle);
        });
    }

    // A seeded random prefix of step/deliver decisions ...
    let mut rng = SplitMix64(seed);
    for _ in 0 .. prefix {
        let decision =
        if rng.next() % 2 == 0 {
            Decision::Step(rng.below(peers))
        }
        else {
            Decision::Deliver {
                source: rng.below(peers),
                target: rng.below(peers),
                count: 1 + rng.below(4),
            }
        };
        sim.apply(decision);
    }

    // ... then a fair drain to completion.
    assert!(sim.drain(10_000), "simulation failed to quiesce");

    let log = Rc::try_unwrap(log).expect("operators should be dropped").into_inner();

    // Progress safety: each worker sees every round exactly once, in order.
    for worker in 0 .. peers {
        let rounds: Vec<usize> = log.iter().filter(|(w, _)| *w == worker).map(|(_, r)| *r).collect();
        assert_eq!(rounds, (0 .. ROUNDS).collect::<Vec<_>>(), "worker {}", worker);
    }

    log
}

#[test]
fn barrier_completes_under_random_schedules() {
    for seed in 0 .. 32 {
        barrier_run(3, seed, 2_000);
    }
}

#[test]
fn barrier_under_wide_schedules() {
    for seed in 0 .. 8 {
        barrier_run(6, seed, 4_000);
    }
}

#[test]
fn same_seed_same_execution() {
    let first = barrier_run(4, 0xDECAF, 3_000);
    let second = barrier_run(4, 0xDECAF, 3_000);
    assert_eq!(first, second);
}

#[test]
fn starved_worker_stalls_then_completes() {
    // Only ever step worker 0 and deliver into it; nobody else runs until the drain.
    let mut sim = Simulation::new(3);
    let reached = Rc::new(RefCell::new(vec![0_usize; 3]));
    for index in 0 .. 3 {
        let reached = Rc::clone(&reached);
        sim.worker_mut(index).dataflow(move |scope| {
            let (handle, stream) = scope.feedback::<Vec<usize>>(1);
            stream.unary_notify::<CapacityContainerBuilder<_>, _, _>(
                Pipeline,
                "Barrier",
                vec![0],
                move |_, _, notificator| {
                    while let Some((cap, _cnt)) = notificator.next() {
                        let round = *cap.time();
                        reached.borrow_mut()[index] = round;
                        if round + 1 < ROUNDS {
                            notificator.notify_at(cap.delayed(&(round + 1)));
                        }
                    }
                }
            )
            .connect_loop(handle);
        });
    }

    for _ in 0 .. 1_000 {
        sim.apply(Decision::Step(0));
        for source in 0 .. 3 {
            sim.apply(Decision::Deliver { source, target: 0, count: usize::MAX });
        }
    }

    // Worker 0 cannot pass round 0: peers have neither run nor confirmed progress.
    assert_eq!(reached.borrow()[0], 0);
    assert!(sim.drain(10_000), "simulation failed to quiesce");
    assert_eq!(*reached.borrow(), vec![ROUNDS - 1; 3]);
}
