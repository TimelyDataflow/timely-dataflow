//! Benchmark harness comparing three intra-process progress-broadcast structures:
//!
//! 1. **MPSC baseline**: per-reader `Mutex<VecDeque<Delta>>`; send clones the
//!    delta into every reader's queue; recv drains its own queue and folds.
//!    This models the current Progcaster intra-process path (per-peer clone,
//!    no in-transit merging).
//! 2. **Chain**: the multi-writer compacting chain (`timely_communication::chain`),
//!    where concurrent sends merge — and cancel — at the shared head.
//! 3. **Mesh**: per-writer chains, retained to demonstrate the cross-writer
//!    cancellation pathology empirically.
//!
//! The delta type is a miniature ChangeBatch: `Vec<(u64, i64)>` with merge =
//! append + amortized consolidation (sort, sum duplicates, drop zeros), mimicking
//! timely's progress updates, including cancellation.
//!
//! Workload: N workers, all-to-all (each worker is one writer and one reader).
//! At step `s`, worker `w` mints `{(t(s, w), +1)}` with `t(s, w) = s * N + w`,
//! and (for a configurable fraction of steps) also retires its predecessor's mint
//! from the previous step, `{(t(s-1, (w-1) mod N), -1)}`, so the all-worker union
//! over a window largely cancels.
//!
//! Scenarios (run for each N, cancellation fraction, and structure):
//! - A. ALL KEEP UP: every worker recvs every step; wall time and sends/sec.
//! - B. LAGGARD: worker 0 recvs every K = 1024 steps; others every step; total
//!   wall time, the laggard's fold work (entries folded per recv), and the
//!   laggard's recv latency.
//! - C. UNREAD BACKLOG: nobody recvs until the end; peak retained state and
//!   final catch-up time.
//!
//! Timing rows are the median of three runs after one warmup run.

use std::collections::VecDeque;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use timely_communication::chain::{Chain, Chainable, Mesh, MeshReader, Reader};

/// A miniature ChangeBatch: updates with amortized consolidation.
#[derive(Clone, Default)]
struct Delta {
    updates: Vec<(u64, i64)>,
    /// Consolidation is amortized: we consolidate when the length doubles past
    /// the last consolidated length (as timely's `ChangeBatch` does).
    clean: usize,
}

impl Delta {
    fn from_slice(updates: &[(u64, i64)]) -> Self {
        Delta { updates: updates.to_vec(), clean: 0 }
    }
    fn extend_from(&mut self, other: &[(u64, i64)]) {
        self.updates.extend_from_slice(other);
        if self.updates.len() > 32 && self.updates.len() > 2 * self.clean {
            self.consolidate();
        }
    }
    fn consolidate(&mut self) {
        self.updates.sort_unstable_by_key(|x| x.0);
        let mut write = 0;
        for read in 0 .. self.updates.len() {
            if write > 0 && self.updates[write - 1].0 == self.updates[read].0 {
                self.updates[write - 1].1 += self.updates[read].1;
            }
            else {
                if write > 0 && self.updates[write - 1].1 == 0 { write -= 1; }
                self.updates[write] = self.updates[read];
                write += 1;
            }
        }
        if write > 0 && self.updates[write - 1].1 == 0 { write -= 1; }
        self.updates.truncate(write);
        self.clean = self.updates.len();
    }
    /// The sum of all diffs (used for correctness checks).
    fn diff_sum(&self) -> i64 {
        self.updates.iter().map(|(_, diff)| *diff).sum()
    }
}

impl Chainable for Delta {
    fn merge_from(&mut self, other: &Self) {
        self.extend_from(&other.updates);
    }
}

/// The delta worker `w` sends at step `s`: its own mint, plus (for `cancel_pct`
/// percent of steps) the retirement of its predecessor's mint from step `s - 1`.
fn make_delta(step: u64, worker: u64, workers: u64, cancel_pct: u64) -> Vec<(u64, i64)> {
    let mut updates = vec![(step * workers + worker, 1)];
    if step > 0 && (step % 100) < cancel_pct {
        let prev = (worker + workers - 1) % workers;
        updates.push(((step - 1) * workers + prev, -1));
    }
    updates
}

/// The sum of diffs over all deltas sent by all workers (for correctness checks).
fn expected_diff_sum(steps: u64, workers: u64, cancel_pct: u64) -> i64 {
    let mut total = 0i64;
    for step in 0 .. steps {
        let paired = step > 0 && (step % 100) < cancel_pct;
        total += workers as i64 * if paired { 0 } else { 1 };
    }
    total
}

/// One worker's communication endpoint: a writer plus its own reader.
trait Endpoint: Send {
    /// Broadcasts a delta to all readers.
    fn send(&self, updates: &[(u64, i64)]);
    /// Folds everything unread into `folded`; returns the number of entries folded.
    fn recv(&mut self, folded: &mut Delta) -> usize;
}

/// Per-reader queues; send clones into every queue (the Progcaster model).
struct MpscEndpoint {
    queues: Arc<Vec<Mutex<VecDeque<Delta>>>>,
    index: usize,
}

impl Endpoint for MpscEndpoint {
    fn send(&self, updates: &[(u64, i64)]) {
        for queue in self.queues.iter() {
            queue.lock().unwrap().push_back(Delta::from_slice(updates));
        }
    }
    fn recv(&mut self, folded: &mut Delta) -> usize {
        let drained: Vec<Delta> = {
            let mut queue = self.queues[self.index].lock().unwrap();
            queue.drain(..).collect()
        };
        let mut entries = 0;
        for delta in drained.iter() {
            entries += delta.updates.len();
            folded.extend_from(&delta.updates);
        }
        entries
    }
}

/// A shared multi-writer chain; one reader per worker.
struct ChainEndpoint {
    chain: Chain<Delta>,
    reader: Reader<Delta>,
}

impl Endpoint for ChainEndpoint {
    fn send(&self, updates: &[(u64, i64)]) {
        self.chain.send(Delta::from_slice(updates));
    }
    fn recv(&mut self, folded: &mut Delta) -> usize {
        let mut entries = 0;
        self.reader.recv_with(|delta| {
            entries += delta.updates.len();
            folded.extend_from(&delta.updates);
        });
        entries
    }
}

/// Per-writer chains; each worker writes its own and reads all (the pathology).
struct MeshEndpoint {
    writer: Chain<Delta>,
    reader: MeshReader<Delta>,
}

impl Endpoint for MeshEndpoint {
    fn send(&self, updates: &[(u64, i64)]) {
        self.writer.send(Delta::from_slice(updates));
    }
    fn recv(&mut self, folded: &mut Delta) -> usize {
        let mut entries = 0;
        self.reader.recv_with(|delta| {
            entries += delta.updates.len();
            folded.extend_from(&delta.updates);
        });
        entries
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Structure { Mpsc, Chain, Mesh }

impl Structure {
    fn name(self) -> &'static str {
        match self {
            Structure::Mpsc => "mpsc",
            Structure::Chain => "chain",
            Structure::Mesh => "mesh",
        }
    }
}

const STRUCTURES: [Structure; 3] = [Structure::Mpsc, Structure::Chain, Structure::Mesh];

/// Reports retained state: total entries, and retained "units" (queued deltas
/// for MPSC; live nodes for Chain/Mesh). For the chain structures this folds a
/// probe reader registered before any sends, which observes (without removing)
/// the chain's full retained content.
type ProbeFn = Box<dyn FnMut() -> (usize, usize) + Send>;

/// Builds the endpoints (and, on request, a retained-state probe) for `workers`
/// workers. The probe must only be requested by scenario C: a never-recv-ing
/// probe reader would otherwise pin old chain state and distort scenarios A/B.
fn build(structure: Structure, workers: usize, with_probe: bool) -> (Vec<Box<dyn Endpoint>>, Option<ProbeFn>) {
    match structure {
        Structure::Mpsc => {
            let queues: Arc<Vec<Mutex<VecDeque<Delta>>>> =
                Arc::new((0 .. workers).map(|_| Mutex::new(VecDeque::new())).collect());
            let endpoints = (0 .. workers)
                .map(|index| Box::new(MpscEndpoint { queues: Arc::clone(&queues), index }) as Box<dyn Endpoint>)
                .collect();
            let probe = with_probe.then(|| {
                let queues = Arc::clone(&queues);
                Box::new(move || {
                    let mut entries = 0;
                    let mut units = 0;
                    for queue in queues.iter() {
                        let queue = queue.lock().unwrap();
                        units += queue.len();
                        entries += queue.iter().map(|delta| delta.updates.len()).sum::<usize>();
                    }
                    (entries, units)
                }) as ProbeFn
            });
            (endpoints, probe)
        }
        Structure::Chain => {
            let chain = Chain::<Delta>::new();
            let probe = with_probe.then(|| {
                let mut reader = chain.reader();
                let diagnostic = chain.clone();
                Box::new(move || {
                    let mut entries = 0;
                    reader.recv_with(|delta| entries += delta.updates.len());
                    (entries, diagnostic.live_len())
                }) as ProbeFn
            });
            let endpoints = (0 .. workers)
                .map(|_| Box::new(ChainEndpoint { chain: chain.clone(), reader: chain.reader() }) as Box<dyn Endpoint>)
                .collect();
            (endpoints, probe)
        }
        Structure::Mesh => {
            let (writers, mesh) = Mesh::<Delta>::new(workers);
            let probe_reader = with_probe.then(|| mesh.reader());
            let endpoints: Vec<Box<dyn Endpoint>> = writers
                .into_iter()
                .map(|writer| Box::new(MeshEndpoint { writer, reader: mesh.reader() }) as Box<dyn Endpoint>)
                .collect();
            let probe = probe_reader.map(|mut reader| {
                Box::new(move || {
                    let mut entries = 0;
                    reader.recv_with(|delta| entries += delta.updates.len());
                    (entries, mesh.live_lens().iter().sum())
                }) as ProbeFn
            });
            (endpoints, probe)
        }
    }
}

fn median_by_wall<M>(mut runs: Vec<(Duration, M)>) -> (Duration, M) {
    runs.sort_by_key(|(wall, _)| *wall);
    runs.remove(runs.len() / 2)
}

/// Scenario A: every worker sends and recvs every step.
fn scenario_a(structure: Structure, workers: usize, steps: u64, cancel_pct: u64) -> Duration {
    let mut runs = Vec::new();
    for run in 0 .. 4 {
        let (endpoints, _) = build(structure, workers, false);
        let barrier = Arc::new(Barrier::new(workers + 1));
        let expected = expected_diff_sum(steps, workers as u64, cancel_pct);
        let threads: Vec<_> = endpoints
            .into_iter()
            .enumerate()
            .map(|(worker, mut endpoint)| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut folded = Delta::default();
                    barrier.wait();
                    for step in 0 .. steps {
                        endpoint.send(&make_delta(step, worker as u64, workers as u64, cancel_pct));
                        endpoint.recv(&mut folded);
                    }
                    barrier.wait();
                    endpoint.recv(&mut folded);
                    folded.consolidate();
                    assert_eq!(folded.diff_sum(), expected);
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        barrier.wait();
        let wall = start.elapsed();
        for thread in threads { thread.join().unwrap(); }
        if run > 0 { runs.push((wall, ())); }
    }
    median_by_wall(runs).0
}

/// Scenario B metrics for the laggard.
#[derive(Clone, Default)]
struct LaggardMetrics {
    recvs: usize,
    entries_total: usize,
    entries_max: usize,
    latency_total: Duration,
    latency_max: Duration,
}

/// Scenario B: worker 0 recvs only every `lag` steps; others every step.
fn scenario_b(structure: Structure, workers: usize, steps: u64, cancel_pct: u64, lag: u64) -> (Duration, LaggardMetrics) {
    let mut runs = Vec::new();
    for run in 0 .. 4 {
        let (endpoints, _) = build(structure, workers, false);
        let barrier = Arc::new(Barrier::new(workers + 1));
        let expected = expected_diff_sum(steps, workers as u64, cancel_pct);
        let metrics = Arc::new(Mutex::new(LaggardMetrics::default()));
        let threads: Vec<_> = endpoints
            .into_iter()
            .enumerate()
            .map(|(worker, mut endpoint)| {
                let barrier = Arc::clone(&barrier);
                let metrics = Arc::clone(&metrics);
                std::thread::spawn(move || {
                    let laggard = worker == 0;
                    let mut local = LaggardMetrics::default();
                    let mut folded = Delta::default();
                    barrier.wait();
                    for step in 0 .. steps {
                        endpoint.send(&make_delta(step, worker as u64, workers as u64, cancel_pct));
                        if !laggard {
                            endpoint.recv(&mut folded);
                        }
                        else if step % lag == lag - 1 {
                            let start = Instant::now();
                            let entries = endpoint.recv(&mut folded);
                            let latency = start.elapsed();
                            local.recvs += 1;
                            local.entries_total += entries;
                            local.entries_max = local.entries_max.max(entries);
                            local.latency_total += latency;
                            local.latency_max = local.latency_max.max(latency);
                        }
                    }
                    barrier.wait();
                    endpoint.recv(&mut folded);
                    folded.consolidate();
                    assert_eq!(folded.diff_sum(), expected);
                    if laggard { *metrics.lock().unwrap() = local; }
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        barrier.wait();
        let wall = start.elapsed();
        for thread in threads { thread.join().unwrap(); }
        if run > 0 {
            let collected = metrics.lock().unwrap().clone();
            runs.push((wall, collected));
        }
    }
    median_by_wall(runs)
}

/// Scenario C metrics.
#[derive(Clone, Default)]
struct BacklogMetrics {
    retained_entries: usize,
    retained_units: usize,
    catchup: Duration,
    folded_by_reader0: usize,
}

/// Scenario C: nobody recvs until the end; then everyone catches up at once.
fn scenario_c(structure: Structure, workers: usize, steps: u64, cancel_pct: u64) -> BacklogMetrics {
    let mut runs = Vec::new();
    for run in 0 .. 4 {
        let (endpoints, probe) = build(structure, workers, true);
        let mut probe = probe.expect("probe requested");
        let barrier = Arc::new(Barrier::new(workers + 1));
        let expected = expected_diff_sum(steps, workers as u64, cancel_pct);
        let folded0 = Arc::new(Mutex::new(0usize));
        let threads: Vec<_> = endpoints
            .into_iter()
            .enumerate()
            .map(|(worker, mut endpoint)| {
                let barrier = Arc::clone(&barrier);
                let folded0 = Arc::clone(&folded0);
                std::thread::spawn(move || {
                    barrier.wait();                 // start sending
                    for step in 0 .. steps {
                        endpoint.send(&make_delta(step, worker as u64, workers as u64, cancel_pct));
                    }
                    barrier.wait();                 // all sends done
                    barrier.wait();                 // probe measured; catch up
                    let mut folded = Delta::default();
                    let entries = endpoint.recv(&mut folded);
                    barrier.wait();                 // catch-up done
                    folded.consolidate();
                    assert_eq!(folded.diff_sum(), expected);
                    if worker == 0 { *folded0.lock().unwrap() = entries; }
                })
            })
            .collect();
        barrier.wait();                             // start sending
        barrier.wait();                             // all sends done
        let (retained_entries, retained_units) = probe();
        let start = Instant::now();
        barrier.wait();                             // catch up
        barrier.wait();                             // catch-up done
        let catchup = start.elapsed();
        for thread in threads { thread.join().unwrap(); }
        if run > 0 {
            let metrics = BacklogMetrics {
                retained_entries,
                retained_units,
                catchup,
                folded_by_reader0: *folded0.lock().unwrap(),
            };
            runs.push((catchup, metrics));
        }
    }
    median_by_wall(runs).1
}

fn main() {
    let worker_counts = [2usize, 4, 8];
    let cancel_pcts = [100u64, 50, 0];

    let steps_a: u64 = 400_000;
    let steps_b: u64 = 400_000;
    let steps_c: u64 = 50_000;
    let lag: u64 = 1024;

    println!("# chain_bench results");
    println!();
    println!("Delta: miniature ChangeBatch (`Vec<(u64, i64)>`, amortized consolidation).");
    println!("Workload: all-to-all; worker w mints (t,+1) each step; its successor");
    println!("retires it (-1) one step later for `cancel%` of steps. Timing rows are");
    println!("the median of 3 runs after a warmup run.");
    println!();

    // Scenario A.
    println!("## Scenario A: all keep up ({} steps/worker; send + recv every step)", steps_a);
    println!();
    println!("| N | cancel% | structure | wall (s) | sends/sec |");
    println!("|---|---------|-----------|----------|-----------|");
    for &workers in &worker_counts {
        for &cancel_pct in &cancel_pcts {
            for &structure in &STRUCTURES {
                let wall = scenario_a(structure, workers, steps_a, cancel_pct);
                let rate = (workers as u64 * steps_a) as f64 / wall.as_secs_f64();
                println!("| {} | {} | {} | {:.3} | {:.0} |", workers, cancel_pct, structure.name(), wall.as_secs_f64(), rate);
            }
        }
    }
    println!();

    // Scenario B.
    println!("## Scenario B: laggard ({} steps/worker; worker 0 recvs every {} steps)", steps_b, lag);
    println!();
    println!("| N | cancel% | structure | wall (s) | laggard recvs | mean entries/recv | max entries/recv | mean latency (µs) | max latency (µs) |");
    println!("|---|---------|-----------|----------|---------------|-------------------|------------------|-------------------|------------------|");
    for &workers in &worker_counts {
        for &cancel_pct in &cancel_pcts {
            for &structure in &STRUCTURES {
                let (wall, metrics) = scenario_b(structure, workers, steps_b, cancel_pct, lag);
                let recvs = metrics.recvs.max(1);
                println!(
                    "| {} | {} | {} | {:.3} | {} | {:.0} | {} | {:.1} | {:.1} |",
                    workers, cancel_pct, structure.name(), wall.as_secs_f64(),
                    metrics.recvs,
                    metrics.entries_total as f64 / recvs as f64,
                    metrics.entries_max,
                    metrics.latency_total.as_secs_f64() * 1e6 / recvs as f64,
                    metrics.latency_max.as_secs_f64() * 1e6,
                );
            }
        }
    }
    println!();

    // Scenario C.
    println!("## Scenario C: unread backlog ({} steps/worker; recv only at the end)", steps_c);
    println!();
    println!("Retained units: queued deltas (mpsc) or live chain nodes (chain/mesh).");
    println!();
    println!("| N | cancel% | structure | retained entries | retained units | folded by reader 0 | catch-up (s) |");
    println!("|---|---------|-----------|------------------|----------------|--------------------|--------------|");
    for &workers in &worker_counts {
        for &cancel_pct in &cancel_pcts {
            for &structure in &STRUCTURES {
                let metrics = scenario_c(structure, workers, steps_c, cancel_pct);
                println!(
                    "| {} | {} | {} | {} | {} | {} | {:.3} |",
                    workers, cancel_pct, structure.name(),
                    metrics.retained_entries, metrics.retained_units,
                    metrics.folded_by_reader0, metrics.catchup.as_secs_f64(),
                );
            }
        }
    }
}
