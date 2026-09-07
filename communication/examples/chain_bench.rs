//! Benchmark harness comparing five intra-process progress-broadcast structures:
//!
//! 1. **MPSC baseline**: per-reader `Mutex<VecDeque<Delta>>`; send clones the
//!    delta into every reader's queue; recv drains its own queue and folds.
//!    This models the current Progcaster intra-process path (per-peer clone,
//!    no in-transit merging).
//! 2. **Chain**: the multi-writer compacting chain (`timely_communication::chain`),
//!    where concurrent sends merge — and cancel — at the shared head.
//! 3. **Mesh**: per-writer chains, retained to demonstrate the cross-writer
//!    cancellation pathology empirically.
//! 4. **Ledger**: the maximal-merging end of the spectrum. One mutex-protected
//!    consolidated all-time sum of every submitted delta (with cancellation this
//!    *is* the outstanding counts, so it stays small), plus an atomic version
//!    bumped per submit so readers can cheaply skip when nothing changed. A
//!    reader recvs by diffing the shared state against its own local copy
//!    (a sorted merge-walk) and folding the diff.
//! 5. **Cells**: per-reader accumulator cells — the in-place accumulation
//!    variant of MPSC. One `Mutex<Delta>` per reader; submit merges the delta
//!    into every reader's cell (consolidating, so cross-writer cancellation
//!    happens per cell); recv takes its own cell wholesale and folds it. A
//!    per-cell atomic version makes an idle recv a single atomic load.
//!
//! The delta type is a miniature ChangeBatch: `Vec<(K, i64)>` with merge =
//! append + amortized consolidation (sort, sum duplicates, drop zeros), mimicking
//! timely's progress updates, including cancellation.
//!
//! The key type `K` is a second axis: `u64` (timely's pointstamps-as-integers
//! stand-in), or an "alloc" key (`Box<[u64; 3]>` wrapping the same logical u64)
//! modeling DD/MZ Pointstamp timestamps that allocate — same comparison and
//! cancellation structure, but every clone allocates and every drop frees,
//! possibly on a different thread than the allocation.
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
//! Modes: no argument runs the full N × cancel% matrix with `u64` keys;
//! `alloc` runs the five-structure × two-key comparison (scenarios A and B at
//! N ∈ {4, 8}, cancel 100%; scenario C at N = 8, cancel 100%); `smoke` runs a
//! tiny correctness pass over every structure, scenario, and key type.
//!
//! Timing rows are the median of three runs after one warmup run.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use timely_communication::chain::{Chain, Chainable, Mesh, MeshReader, Reader};

/// The timestamp key: ordered, clonable, and constructible from a logical u64.
trait Key: Ord + Clone + Send + Sync + 'static {
    const NAME: &'static str;
    fn from_time(time: u64) -> Self;
}

impl Key for u64 {
    const NAME: &'static str = "u64";
    fn from_time(time: u64) -> Self { time }
}

/// A heap-bearing key modeling DD/MZ Pointstamp timestamps that allocate:
/// the same ordering and cancellation structure as the wrapped u64, but every
/// clone allocates and every drop frees — and a delta cloned at the sender and
/// consumed by a reader is freed on the reader's thread, so cross-thread free
/// traffic occurs exactly as it would in production.
impl Key for Box<[u64; 3]> {
    const NAME: &'static str = "alloc";
    fn from_time(time: u64) -> Self { Box::new([time, 0, 0]) }
}

/// A miniature ChangeBatch: updates with amortized consolidation.
#[derive(Clone)]
struct Delta<K: Key> {
    updates: Vec<(K, i64)>,
    /// Consolidation is amortized: we consolidate when the length doubles past
    /// the last consolidated length (as timely's `ChangeBatch` does).
    clean: usize,
}

impl<K: Key> Default for Delta<K> {
    fn default() -> Self {
        Delta { updates: Vec::new(), clean: 0 }
    }
}

impl<K: Key> Delta<K> {
    fn from_updates(updates: &[(K, i64)]) -> Self {
        Delta { updates: updates.to_vec(), clean: 0 }
    }
    fn maybe_consolidate(&mut self) {
        if self.updates.len() > 32 && self.updates.len() > 2 * self.clean {
            self.consolidate();
        }
    }
    /// Extends by cloning from a slice (the cross-endpoint broadcast path).
    fn extend_from(&mut self, other: &[(K, i64)]) {
        self.updates.extend_from_slice(other);
        self.maybe_consolidate();
    }
    /// Extends by moving owned updates (the fold path for structures whose
    /// recv obtains ownership); the receiver then drops what it consumed.
    fn extend_owned(&mut self, other: Vec<(K, i64)>) {
        self.updates.extend(other);
        self.maybe_consolidate();
    }
    fn consolidate(&mut self) {
        self.updates.sort_unstable_by(|x, y| x.0.cmp(&y.0));
        let mut write = 0;
        for read in 0 .. self.updates.len() {
            if write > 0 && self.updates[write - 1].0 == self.updates[read].0 {
                self.updates[write - 1].1 += self.updates[read].1;
            }
            else {
                if write > 0 && self.updates[write - 1].1 == 0 { write -= 1; }
                self.updates.swap(write, read);
                write += 1;
            }
        }
        if write > 0 && self.updates[write - 1].1 == 0 { write -= 1; }
        self.updates.truncate(write);
        self.clean = self.updates.len();
    }
    /// Incrementally consolidates the unconsolidated tail into the (sorted,
    /// zero-free) consolidated prefix, leaving the whole fully consolidated.
    /// Cost is O(tail + overlap) rather than a full re-sort, which matters for
    /// the ledger: its shared state is diffed (and so must be consolidated) on
    /// every recv, while submits only append near the tail.
    fn consolidate_tail(&mut self) {
        if self.updates.len() == self.clean { return; }
        let mut tail = self.updates.split_off(self.clean);
        tail.sort_unstable_by(|x, y| x.0.cmp(&y.0));
        let pos = self.updates.partition_point(|x| x.0 < tail[0].0);
        let mut prefix = self.updates.split_off(pos).into_iter().peekable();
        let mut tail = tail.into_iter().peekable();
        loop {
            let entry = match (prefix.peek(), tail.peek()) {
                (Some(p), Some(t)) => {
                    if p.0 <= t.0 { prefix.next().unwrap() } else { tail.next().unwrap() }
                }
                (Some(_), None) => prefix.next().unwrap(),
                (None, Some(_)) => tail.next().unwrap(),
                (None, None) => break,
            };
            match self.updates.last_mut() {
                Some(last) if last.0 == entry.0 => {
                    last.1 += entry.1;
                    if last.1 == 0 { self.updates.pop(); }
                }
                _ => {
                    if entry.1 != 0 { self.updates.push(entry); }
                }
            }
        }
        self.clean = self.updates.len();
    }
    /// The sum of all diffs (used for correctness checks).
    fn diff_sum(&self) -> i64 {
        self.updates.iter().map(|(_, diff)| *diff).sum()
    }
}

impl<K: Key> Chainable for Delta<K> {
    fn merge_from(&mut self, other: &Self) {
        self.extend_from(&other.updates);
    }
}

/// The delta worker `w` sends at step `s`: its own mint, plus (for `cancel_pct`
/// percent of steps) the retirement of its predecessor's mint from step `s - 1`.
fn make_delta<K: Key>(step: u64, worker: u64, workers: u64, cancel_pct: u64) -> Vec<(K, i64)> {
    let mut updates = vec![(K::from_time(step * workers + worker), 1)];
    if step > 0 && (step % 100) < cancel_pct {
        let prev = (worker + workers - 1) % workers;
        updates.push((K::from_time((step - 1) * workers + prev), -1));
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
trait Endpoint<K: Key>: Send {
    /// Broadcasts a delta to all readers.
    fn send(&self, updates: &[(K, i64)]);
    /// Folds everything unread into `folded`; returns the number of entries folded.
    fn recv(&mut self, folded: &mut Delta<K>) -> usize;
}

/// Per-reader queues; send clones into every queue (the Progcaster model).
struct MpscEndpoint<K: Key> {
    queues: Arc<Vec<Mutex<VecDeque<Delta<K>>>>>,
    index: usize,
}

impl<K: Key> Endpoint<K> for MpscEndpoint<K> {
    fn send(&self, updates: &[(K, i64)]) {
        for queue in self.queues.iter() {
            queue.lock().unwrap().push_back(Delta::from_updates(updates));
        }
    }
    fn recv(&mut self, folded: &mut Delta<K>) -> usize {
        let drained: Vec<Delta<K>> = {
            let mut queue = self.queues[self.index].lock().unwrap();
            queue.drain(..).collect()
        };
        let mut entries = 0;
        for delta in drained {
            entries += delta.updates.len();
            folded.extend_owned(delta.updates);
        }
        entries
    }
}

/// A shared multi-writer chain; one reader per worker.
struct ChainEndpoint<K: Key> {
    chain: Chain<Delta<K>>,
    reader: Reader<Delta<K>>,
}

impl<K: Key> Endpoint<K> for ChainEndpoint<K> {
    fn send(&self, updates: &[(K, i64)]) {
        self.chain.send(Delta::from_updates(updates));
    }
    fn recv(&mut self, folded: &mut Delta<K>) -> usize {
        let mut entries = 0;
        self.reader.recv_with(|delta| {
            entries += delta.updates.len();
            folded.extend_from(&delta.updates);
        });
        entries
    }
}

/// Per-writer chains; each worker writes its own and reads all (the pathology).
struct MeshEndpoint<K: Key> {
    writer: Chain<Delta<K>>,
    reader: MeshReader<Delta<K>>,
}

impl<K: Key> Endpoint<K> for MeshEndpoint<K> {
    fn send(&self, updates: &[(K, i64)]) {
        self.writer.send(Delta::from_updates(updates));
    }
    fn recv(&mut self, folded: &mut Delta<K>) -> usize {
        let mut entries = 0;
        self.reader.recv_with(|delta| {
            entries += delta.updates.len();
            folded.extend_from(&delta.updates);
        });
        entries
    }
}

/// One mutex-protected consolidated sum of every submitted delta, plus an
/// atomic version bumped per submit so readers can cheaply skip when nothing
/// has changed since their last recv.
struct LedgerShared<K: Key> {
    state: Mutex<Delta<K>>,
    version: AtomicUsize,
}

/// The shared ledger plus a reader-local copy of the consolidated state (kept
/// outside the lock) and the version latched at the reader's last recv.
struct LedgerEndpoint<K: Key> {
    shared: Arc<LedgerShared<K>>,
    local: Vec<(K, i64)>,
    seen: usize,
}

impl<K: Key> Endpoint<K> for LedgerEndpoint<K> {
    fn send(&self, updates: &[(K, i64)]) {
        let mut state = self.shared.state.lock().unwrap();
        state.extend_from(updates);
        self.shared.version.fetch_add(1, Ordering::Release);
    }
    fn recv(&mut self, folded: &mut Delta<K>) -> usize {
        if self.shared.version.load(Ordering::Acquire) == self.seen {
            return 0;
        }
        let mut diff = Vec::new();
        {
            let mut state = self.shared.state.lock().unwrap();
            self.seen = self.shared.version.load(Ordering::Acquire);
            state.consolidate_tail();
            // Merge-walk the (sorted, zero-free) shared and local states,
            // emitting entries whose counts differ as (key, shared - local).
            let shared = &state.updates;
            let local = &self.local;
            let (mut i, mut j) = (0, 0);
            while i < shared.len() || j < local.len() {
                if j >= local.len() || (i < shared.len() && shared[i].0 < local[j].0) {
                    diff.push(shared[i].clone());
                    i += 1;
                }
                else if i >= shared.len() || local[j].0 < shared[i].0 {
                    diff.push((local[j].0.clone(), -local[j].1));
                    j += 1;
                }
                else {
                    if shared[i].1 != local[j].1 {
                        diff.push((shared[i].0.clone(), shared[i].1 - local[j].1));
                    }
                    i += 1;
                    j += 1;
                }
            }
            self.local.clone_from(&state.updates);
        }
        let entries = diff.len();
        folded.extend_owned(diff);
        entries
    }
}

/// One per-reader accumulator cell: in-place consolidated state plus a version
/// bumped per submit so an idle recv is a single atomic load.
struct Cell<K: Key> {
    state: Mutex<Delta<K>>,
    version: AtomicUsize,
}

/// Per-reader accumulator cells: submit merges (consolidating, so cross-writer
/// cancellation happens per cell) into every reader's cell; recv takes its own
/// cell wholesale and folds it.
struct CellsEndpoint<K: Key> {
    cells: Arc<Vec<Cell<K>>>,
    index: usize,
    seen: usize,
}

impl<K: Key> Endpoint<K> for CellsEndpoint<K> {
    fn send(&self, updates: &[(K, i64)]) {
        for cell in self.cells.iter() {
            cell.state.lock().unwrap().extend_from(updates);
            cell.version.fetch_add(1, Ordering::Release);
        }
    }
    fn recv(&mut self, folded: &mut Delta<K>) -> usize {
        let cell = &self.cells[self.index];
        if cell.version.load(Ordering::Acquire) == self.seen {
            return 0;
        }
        let taken = {
            let mut state = cell.state.lock().unwrap();
            self.seen = cell.version.load(Ordering::Acquire);
            std::mem::take(&mut *state)
        };
        let entries = taken.updates.len();
        folded.extend_owned(taken.updates);
        entries
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Structure { Mpsc, Chain, Mesh, Ledger, Cells }

impl Structure {
    fn name(self) -> &'static str {
        match self {
            Structure::Mpsc => "mpsc",
            Structure::Chain => "chain",
            Structure::Mesh => "mesh",
            Structure::Ledger => "ledger",
            Structure::Cells => "cells",
        }
    }
}

const STRUCTURES: [Structure; 5] = [Structure::Mpsc, Structure::Chain, Structure::Mesh, Structure::Ledger, Structure::Cells];
/// Ledger-free subset for the alloc matrix (the ledger's O(live-state) recv is too slow here).
const ALLOC_STRUCTURES: [Structure; 4] = [Structure::Mpsc, Structure::Chain, Structure::Mesh, Structure::Cells];

/// Reports retained state: total entries, and retained "units" (queued deltas
/// for MPSC; live nodes for Chain/Mesh; the single ledger; cells for Cells).
/// For the chain structures this folds a probe reader registered before any
/// sends, which observes (without removing) the chain's full retained content.
type ProbeFn = Box<dyn FnMut() -> (usize, usize) + Send>;

/// Builds the endpoints (and, on request, a retained-state probe) for `workers`
/// workers. The probe must only be requested by scenario C: a never-recv-ing
/// probe reader would otherwise pin old chain state and distort scenarios A/B.
fn build<K: Key>(structure: Structure, workers: usize, with_probe: bool) -> (Vec<Box<dyn Endpoint<K>>>, Option<ProbeFn>) {
    match structure {
        Structure::Mpsc => {
            let queues: Arc<Vec<Mutex<VecDeque<Delta<K>>>>> =
                Arc::new((0 .. workers).map(|_| Mutex::new(VecDeque::new())).collect());
            let endpoints = (0 .. workers)
                .map(|index| Box::new(MpscEndpoint { queues: Arc::clone(&queues), index }) as Box<dyn Endpoint<K>>)
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
            let chain = Chain::<Delta<K>>::new();
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
                .map(|_| Box::new(ChainEndpoint { chain: chain.clone(), reader: chain.reader() }) as Box<dyn Endpoint<K>>)
                .collect();
            (endpoints, probe)
        }
        Structure::Mesh => {
            let (writers, mesh) = Mesh::<Delta<K>>::new(workers);
            let probe_reader = with_probe.then(|| mesh.reader());
            let endpoints: Vec<Box<dyn Endpoint<K>>> = writers
                .into_iter()
                .map(|writer| Box::new(MeshEndpoint { writer, reader: mesh.reader() }) as Box<dyn Endpoint<K>>)
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
        Structure::Ledger => {
            let shared = Arc::new(LedgerShared {
                state: Mutex::new(Delta::default()),
                version: AtomicUsize::new(0),
            });
            let endpoints = (0 .. workers)
                .map(|_| Box::new(LedgerEndpoint {
                    shared: Arc::clone(&shared),
                    local: Vec::new(),
                    seen: 0,
                }) as Box<dyn Endpoint<K>>)
                .collect();
            let probe = with_probe.then(|| {
                let shared = Arc::clone(&shared);
                Box::new(move || {
                    let mut state = shared.state.lock().unwrap();
                    state.consolidate_tail();
                    (state.updates.len(), 1usize)
                }) as ProbeFn
            });
            (endpoints, probe)
        }
        Structure::Cells => {
            let cells: Arc<Vec<Cell<K>>> = Arc::new(
                (0 .. workers)
                    .map(|_| Cell { state: Mutex::new(Delta::default()), version: AtomicUsize::new(0) })
                    .collect(),
            );
            let endpoints = (0 .. workers)
                .map(|index| Box::new(CellsEndpoint { cells: Arc::clone(&cells), index, seen: 0 }) as Box<dyn Endpoint<K>>)
                .collect();
            let probe = with_probe.then(|| {
                let cells = Arc::clone(&cells);
                Box::new(move || {
                    let mut entries = 0;
                    for cell in cells.iter() {
                        let mut state = cell.state.lock().unwrap();
                        state.consolidate_tail();
                        entries += state.updates.len();
                    }
                    (entries, cells.len())
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
fn scenario_a<K: Key>(structure: Structure, workers: usize, steps: u64, cancel_pct: u64) -> Duration {
    let mut runs = Vec::new();
    for run in 0 .. 4 {
        let (endpoints, _) = build::<K>(structure, workers, false);
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
                        endpoint.send(&make_delta::<K>(step, worker as u64, workers as u64, cancel_pct));
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
fn scenario_b<K: Key>(structure: Structure, workers: usize, steps: u64, cancel_pct: u64, lag: u64) -> (Duration, LaggardMetrics) {
    let mut runs = Vec::new();
    for run in 0 .. 4 {
        let (endpoints, _) = build::<K>(structure, workers, false);
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
                        endpoint.send(&make_delta::<K>(step, worker as u64, workers as u64, cancel_pct));
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
fn scenario_c<K: Key>(structure: Structure, workers: usize, steps: u64, cancel_pct: u64) -> BacklogMetrics {
    let mut runs = Vec::new();
    for run in 0 .. 4 {
        let (endpoints, probe) = build::<K>(structure, workers, true);
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
                        endpoint.send(&make_delta::<K>(step, worker as u64, workers as u64, cancel_pct));
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

/// The default mode: the full N × cancel% matrix, `u64` keys.
fn run_full_matrix() {
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

    // The ledger's recv diffs the full consolidated shared state, which at
    // cancel% < 100 grows linearly with elapsed sends, making keep-up
    // scenarios quadratic in steps. Those cells run with reduced steps (the
    // table reports steps per cell; sends/sec remains the comparable rate,
    // though it flatters the ledger, whose rate degrades as state grows).
    let cell_steps = |structure: Structure, cancel_pct: u64, steps: u64| {
        if structure == Structure::Ledger && cancel_pct < 100 { steps / 16 } else { steps }
    };

    // Scenario A.
    println!("## Scenario A: all keep up ({} steps/worker; send + recv every step)", steps_a);
    println!();
    println!("| N | cancel% | structure | steps | wall (s) | sends/sec |");
    println!("|---|---------|-----------|-------|----------|-----------|");
    for &workers in &worker_counts {
        for &cancel_pct in &cancel_pcts {
            for &structure in &STRUCTURES {
                let steps = cell_steps(structure, cancel_pct, steps_a);
                let wall = scenario_a::<u64>(structure, workers, steps, cancel_pct);
                let rate = (workers as u64 * steps) as f64 / wall.as_secs_f64();
                println!("| {} | {} | {} | {} | {:.3} | {:.0} |", workers, cancel_pct, structure.name(), steps, wall.as_secs_f64(), rate);
            }
        }
    }
    println!();

    // Scenario B.
    println!("## Scenario B: laggard ({} steps/worker; worker 0 recvs every {} steps)", steps_b, lag);
    println!();
    println!("| N | cancel% | structure | steps | wall (s) | laggard recvs | mean entries/recv | max entries/recv | mean latency (µs) | max latency (µs) |");
    println!("|---|---------|-----------|-------|----------|---------------|-------------------|------------------|-------------------|------------------|");
    for &workers in &worker_counts {
        for &cancel_pct in &cancel_pcts {
            for &structure in &STRUCTURES {
                let steps = cell_steps(structure, cancel_pct, steps_b);
                let (wall, metrics) = scenario_b::<u64>(structure, workers, steps, cancel_pct, lag);
                print_b_row(&format!("| {} | {} | {} | {}", workers, cancel_pct, structure.name(), steps), wall, &metrics);
            }
        }
    }
    println!();

    // Scenario C.
    println!("## Scenario C: unread backlog ({} steps/worker; recv only at the end)", steps_c);
    println!();
    println!("Retained units: queued deltas (mpsc), live chain nodes (chain/mesh),");
    println!("the single shared ledger (ledger; its retained entries are the");
    println!("consolidated shared state size = the net in-flight window), or");
    println!("per-reader cells (cells; retained entries are the sum of cell sizes).");
    println!();
    println!("| N | cancel% | structure | retained entries | retained units | folded by reader 0 | catch-up (s) |");
    println!("|---|---------|-----------|------------------|----------------|--------------------|--------------|");
    for &workers in &worker_counts {
        for &cancel_pct in &cancel_pcts {
            for &structure in &STRUCTURES {
                let metrics = scenario_c::<u64>(structure, workers, steps_c, cancel_pct);
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

fn print_b_row(prefix: &str, wall: Duration, metrics: &LaggardMetrics) {
    let recvs = metrics.recvs.max(1);
    println!(
        "{} | {:.3} | {} | {:.0} | {} | {:.1} | {:.1} |",
        prefix, wall.as_secs_f64(),
        metrics.recvs,
        metrics.entries_total as f64 / recvs as f64,
        metrics.entries_max,
        metrics.latency_total.as_secs_f64() * 1e6 / recvs as f64,
        metrics.latency_max.as_secs_f64() * 1e6,
    );
}

fn alloc_rows_a<K: Key>(workers: usize, steps: u64, cancel_pct: u64) {
    for &structure in &ALLOC_STRUCTURES {
        let wall = scenario_a::<K>(structure, workers, steps, cancel_pct);
        let rate = (workers as u64 * steps) as f64 / wall.as_secs_f64();
        println!("| {} | {} | {} | {:.3} | {:.0} |", workers, K::NAME, structure.name(), wall.as_secs_f64(), rate);
    }
}

fn alloc_rows_b<K: Key>(workers: usize, steps: u64, cancel_pct: u64, lag: u64) {
    for &structure in &ALLOC_STRUCTURES {
        let (wall, metrics) = scenario_b::<K>(structure, workers, steps, cancel_pct, lag);
        print_b_row(&format!("| {} | {} | {}", workers, K::NAME, structure.name()), wall, &metrics);
    }
}

fn alloc_rows_c<K: Key>(workers: usize, steps: u64, cancel_pct: u64) {
    for &structure in &ALLOC_STRUCTURES {
        let metrics = scenario_c::<K>(structure, workers, steps, cancel_pct);
        println!(
            "| {} | {} | {} | {} | {} | {} | {:.3} |",
            workers, K::NAME, structure.name(),
            metrics.retained_entries, metrics.retained_units,
            metrics.folded_by_reader0, metrics.catchup.as_secs_f64(),
        );
    }
}

/// The `alloc` mode: five structures × two key types, cancel 100%.
/// Scenarios A and B at N ∈ {4, 8}; scenario C at N = 8.
fn run_alloc_matrix() {
    let steps_a: u64 = 400_000;
    let steps_b: u64 = 400_000;
    let steps_c: u64 = 50_000;
    let lag: u64 = 1024;
    let cancel_pct: u64 = 100;

    println!("### Scenario A: all keep up ({} steps/worker; cancel {}%)", steps_a, cancel_pct);
    println!();
    println!("| N | key | structure | wall (s) | sends/sec |");
    println!("|---|-----|-----------|----------|-----------|");
    for &workers in &[4usize, 8] {
        alloc_rows_a::<u64>(workers, steps_a, cancel_pct);
        alloc_rows_a::<Box<[u64; 3]>>(workers, steps_a, cancel_pct);
    }
    println!();

    println!("### Scenario B: laggard ({} steps/worker; worker 0 recvs every {} steps; cancel {}%)", steps_b, lag, cancel_pct);
    println!();
    println!("| N | key | structure | wall (s) | laggard recvs | mean entries/recv | max entries/recv | mean latency (µs) | max latency (µs) |");
    println!("|---|-----|-----------|----------|---------------|-------------------|------------------|-------------------|------------------|");
    for &workers in &[4usize, 8] {
        alloc_rows_b::<u64>(workers, steps_b, cancel_pct, lag);
        alloc_rows_b::<Box<[u64; 3]>>(workers, steps_b, cancel_pct, lag);
    }
    println!();

    println!("### Scenario C: unread backlog ({} steps/worker; cancel {}%)", steps_c, cancel_pct);
    println!();
    println!("| N | key | structure | retained entries | retained units | folded by reader 0 | catch-up (s) |");
    println!("|---|-----|-----------|------------------|----------------|--------------------|--------------|");
    alloc_rows_c::<u64>(8, steps_c, cancel_pct);
    alloc_rows_c::<Box<[u64; 3]>>(8, steps_c, cancel_pct);
}

/// The `smoke` mode: a tiny correctness pass (the scenarios assert the folded
/// diff sum) over every structure, scenario, and key type.
fn run_smoke() {
    for &workers in &[2usize, 4] {
        for &cancel_pct in &[100u64, 50, 0] {
            for &structure in &STRUCTURES {
                scenario_a::<u64>(structure, workers, 2_000, cancel_pct);
                scenario_a::<Box<[u64; 3]>>(structure, workers, 2_000, cancel_pct);
                scenario_b::<u64>(structure, workers, 2_000, cancel_pct, 64);
                scenario_b::<Box<[u64; 3]>>(structure, workers, 2_000, cancel_pct, 64);
                scenario_c::<u64>(structure, workers, 2_000, cancel_pct);
                scenario_c::<Box<[u64; 3]>>(structure, workers, 2_000, cancel_pct);
            }
        }
    }
    println!("smoke: all structures × scenarios × key types passed");
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("alloc") => run_alloc_matrix(),
        Some("smoke") => run_smoke(),
        _ => run_full_matrix(),
    }
}
