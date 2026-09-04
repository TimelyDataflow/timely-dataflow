//! Property-testing grind over simulated schedules.
//!
//! Each run builds a "chaos" dataflow on several workers — records spread over input
//! times, exchanged by value, redistributed in time by a capability-holding `delay`,
//! exchanged again — and executes it under a seeded, policy-biased schedule of worker
//! steps and message deliveries. Oracles checked on every run:
//!
//! - **Frontier safety**: an auditing operator asserts that no record arrives at a
//!   time its input frontier had already passed on a previous scheduling. This is the
//!   observable form of the progress-tracking safety property.
//! - **Conservation**: after a fair drain, the multiset of (time, value) pairs observed
//!   across all workers equals exactly what was introduced (no loss, duplication, or
//!   mistiming through exchange, serialization, and delay).
//! - **Quiescence**: the fair drain terminates within a generous bound.
//! - **Determinism**: identical seeds produce identical observation logs.

use std::rc::Rc;
use std::cell::RefCell;
use std::panic::AssertUnwindSafe;

use timely::simulate::{Decision, Simulation};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{ToStream, Exchange};
use timely::dataflow::operators::vec::Delay;
use timely::dataflow::operators::generic::operator::Operator;
use timely::progress::Antichain;

/// Records per worker, distinct input times, and maximum delay, per run.
const RECORDS: u64 = 40;
const TIMES: u64 = 5;
const DELAYS: u64 = 4;

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

/// A deterministic hash for workload choices (input times, delays, routing).
fn mix(seed: u64, value: u64, salt: u64) -> u64 {
    let mut rng = SplitMix64(seed ^ value.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ salt);
    rng.next()
}

fn initial_time(wseed: u64, value: u64) -> usize { (mix(wseed, value, 0xA) % TIMES) as usize }
fn delay_by(wseed: u64, value: u64) -> usize { (mix(wseed, value, 0xB) % DELAYS) as usize }

/// Schedule-generation policies; diversity of bias matters more than seed count.
#[derive(Clone, Copy, Debug)]
enum Policy {
    /// Even mix of steps and deliveries.
    Uniform,
    /// Mostly steps; messages pile up undelivered.
    StepHeavy,
    /// Mostly deliveries; workers rarely run.
    DeliverHeavy,
    /// Even mix, but one stream never delivers until the drain.
    StarveStream(usize, usize),
    /// Long alternating phases of steps-only and deliveries-only.
    Bursty,
}

fn policy_for(rng: &mut SplitMix64, peers: usize) -> Policy {
    match rng.next() % 5 {
        0 => Policy::Uniform,
        1 => Policy::StepHeavy,
        2 => Policy::DeliverHeavy,
        3 => Policy::StarveStream(rng.below(peers), rng.below(peers)),
        _ => Policy::Bursty,
    }
}

/// Builds the chaos dataflow on each worker; returns the shared observation log.
fn build_chaos(sim: &mut Simulation, peers: usize, wseed: u64) -> Rc<RefCell<Vec<(usize, u64)>>> {

    let results = Rc::new(RefCell::new(Vec::new()));

    for index in 0 .. peers {
        let results = Rc::clone(&results);
        sim.worker_mut(index).dataflow::<usize,_,_>(move |scope| {
            let base = (index as u64) * RECORDS;
            (base .. base + RECORDS)
                .to_stream(scope)
                .delay(move |v, _t| initial_time(wseed, *v))
                .exchange(move |v| mix(wseed, *v, 0xC))
                .delay(move |v, t| t + delay_by(wseed, *v))
                .exchange(move |v| mix(wseed, *v, 0xD))
                .unary_frontier::<timely::container::CapacityContainerBuilder<Vec<u64>>, _, _, _>(
                    Pipeline,
                    "Auditor",
                    move |_capability, _info| {
                        // The frontier as of the end of the previous scheduling: a
                        // promise that no future record arrives at a time it passed.
                        let mut previous: Antichain<usize> = Antichain::from_elem(0);
                        move |(input, frontier), _output| {
                            input.for_each_time(|time, data| {
                                assert!(
                                    previous.less_equal(time.time()),
                                    "frontier safety violated: records at {:?} after frontier {:?}",
                                    time.time(), previous.elements(),
                                );
                                // Delivered-but-unconsumed records hold the frontier,
                                // so even the current frontier may not pass their time.
                                assert!(
                                    frontier.less_equal(time.time()),
                                    "frontier safety violated: records at {:?} under frontier {:?}",
                                    time.time(), frontier.frontier(),
                                );
                                for datum in data.flat_map(|d| d.drain(..)) {
                                    results.borrow_mut().push((*time.time(), datum));
                                }
                            });
                            previous = frontier.frontier().to_owned();
                        }
                    }
                );
        });
    }

    results
}

/// The (time, value) multiset every run must observe, independent of schedule.
fn expected(peers: usize, wseed: u64) -> Vec<(usize, u64)> {
    let mut expected = Vec::new();
    for value in 0 .. (peers as u64) * RECORDS {
        expected.push((initial_time(wseed, value) + delay_by(wseed, value), value));
    }
    expected.sort();
    expected
}

/// Runs one seeded schedule against one seeded workload; returns the observation log.
fn chaos_run(peers: usize, wseed: u64, sseed: u64, prefix: usize) -> Vec<(usize, u64)> {

    let mut sim = Simulation::new(peers);
    let results = build_chaos(&mut sim, peers, wseed);

    let mut rng = SplitMix64(sseed);
    let policy = policy_for(&mut rng, peers);

    for round in 0 .. prefix {
        let step =
        match policy {
            Policy::Uniform => rng.next() % 100 < 50,
            Policy::StepHeavy => rng.next() % 100 < 90,
            Policy::DeliverHeavy => rng.next() % 100 < 10,
            Policy::StarveStream(_, _) => rng.next() % 100 < 50,
            Policy::Bursty => (round / 100) % 2 == 0,
        };
        let decision =
        if step {
            Decision::Step(rng.below(peers))
        }
        else {
            let source = rng.below(peers);
            let target = rng.below(peers);
            if let Policy::StarveStream(s, t) = policy {
                if (source, target) == (s, t) { continue; }
            }
            Decision::Deliver { source, target, count: 1 + rng.below(4) }
        };
        sim.apply(decision);
    }

    assert!(sim.drain(50_000), "simulation failed to quiesce");

    let mut log = Rc::try_unwrap(results).expect("operators should be dropped").into_inner();

    // Conservation: exactly the expected records, at exactly the expected times.
    let mut sorted = log.clone();
    sorted.sort();
    assert_eq!(sorted, expected(peers, wseed), "conservation violated");

    log.sort(); // return in canonical order; arrival order is checked by same-seed runs on raw logs.
    log
}

/// As `chaos_run`, but returns the log in arrival order for determinism comparison.
fn chaos_run_raw(peers: usize, wseed: u64, sseed: u64, prefix: usize) -> Vec<(usize, u64)> {
    let mut sim = Simulation::new(peers);
    let results = build_chaos(&mut sim, peers, wseed);
    let mut rng = SplitMix64(sseed);
    let _policy = policy_for(&mut rng, peers);
    for _ in 0 .. prefix {
        let decision =
        if rng.next() % 2 == 0 { Decision::Step(rng.below(peers)) }
        else {
            Decision::Deliver { source: rng.below(peers), target: rng.below(peers), count: 1 + rng.below(4) }
        };
        sim.apply(decision);
    }
    assert!(sim.drain(50_000), "simulation failed to quiesce");
    Rc::try_unwrap(results).expect("operators should be dropped").into_inner()
}

#[test]
fn chaos_small_grind() {
    for wseed in 0 .. 8 {
        for sseed in 0 .. 8 {
            chaos_run(3, wseed, sseed, 2_000);
        }
    }
}

#[test]
fn chaos_wide_grind() {
    for wseed in 0 .. 4 {
        for sseed in 0 .. 4 {
            chaos_run(5, wseed, sseed, 4_000);
        }
    }
}

#[test]
fn chaos_deterministic() {
    for seed in 0 .. 4 {
        let first = chaos_run_raw(3, seed, seed, 2_000);
        let second = chaos_run_raw(3, seed, seed, 2_000);
        assert_eq!(first, second, "same seed, different execution");
    }
}

/// A larger grind for manual exploration: `GRIND_RUNS=50000 cargo test --release
/// -p timely --test grind -- --ignored --nocapture`.
#[test]
#[ignore]
fn chaos_big_grind() {
    let runs: u64 = std::env::var("GRIND_RUNS").ok().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let mut rng = SplitMix64(0x6E1D);
    for run in 0 .. runs {
        let peers = 2 + rng.below(4);
        let wseed = rng.next();
        let sseed = rng.next();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            chaos_run(peers, wseed, sseed, 3_000);
        }));
        assert!(
            outcome.is_ok(),
            "run {} failed: reproduce with chaos_run({}, {:#x}, {:#x}, 3_000)",
            run, peers, wseed, sseed,
        );
        if run % 1_000 == 0 { println!("{}/{} runs clean", run, runs); }
    }
    println!("{} runs clean", runs);
}
