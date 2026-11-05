//! CB-level spill demonstration, complementing `spill_compare.rs`.
//!
//! `spill_compare.rs` exercises *transport-level* spill: the zero-copy
//! `MergeQueue` policy in the communication fabric writes serialized bytes
//! to disk when the in-flight queue grows past a threshold. That works
//! only on the `ProcessBinary` (Bytes) path and treats the payload as
//! opaque bytes.
//!
//! This example takes the alternative approach we discussed: keep
//! transport dumb, push the spill machinery into a custom
//! `ContainerBuilder` used *only on the distributor side of an Exchange
//! pact*. The output port of the producing operator uses a normal,
//! lean CB (never spills); the distributor's per-destination CB spills
//! settled, post-partition batches when a worker-scope memory budget is
//! exceeded. The container carried over channels is a `MaybeSpilled<C>`
//! wrapper that materializes lazily on `drain`.
//!
//! Run (Process flavor; same-process workers, no wire serialization):
//!
//!   cargo run --release --example spill_cb -- --with-spill -w 2
//!   cargo run --release --example spill_cb -- -w 2          # no spill
//!
//! Options:
//!   --total-gb N         Total cluster-wide data (default: 8)
//!   --chunk-kb N         Size of each Vec<u8> sent (default: 256)
//!   --records-per-batch N  Records per built container (default: 64)
//!   --workers N          Number of worker threads (default: 2)
//!   --budget-mb N        Per-CB Live-bytes budget (default: 64)
//!   --spill-dir PATH     Directory for tempfiles (default: std::env::temp_dir())
//!   --with-spill         Enable spill in the distributor CB
//!   --rss-every-secs N   RSS sampling cadence (default: 2)
//!
//! Notes on what is and isn't core-Timely:
//!
//!   * `MaybeSpilled<C>`, `SpillingCB<C>`, the spill provider, and the
//!     worker-scope `SpillEnv` are downstream code, defined inside this
//!     example file. None of them require changes in `timely` proper.
//!   * Wiring into Timely uses only existing public APIs:
//!       - `ExchangeCore::new_core::<SpillingCB<_>, _>(...)` selects the
//!         distributor CB.
//!       - Operator output port uses the default
//!         `CapacityContainerBuilder<MaybeSpilled<_>>` (lean variant,
//!         spill disabled by construction since its in-flight budget is
//!         infinite).
//!       - `InputHandle::send_batch` pushes a pre-built
//!         `MaybeSpilled::Live(_)` container into the dataflow.
//!   * `MaybeSpilled<C>` carries a stub `ContainerBytes` impl: required
//!     by the pact's trait bounds, never actually invoked because the
//!     example uses `CommunicationConfig::Process` (mpsc-Typed flavor,
//!     no wire serialization).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use timely::communication::{Pull, Push};
use timely::container::{CapacityContainerBuilder, ContainerBuilder, LengthPreservingContainerBuilder, DrainContainer, PushInto, Accountable};
use timely::dataflow::InputHandle;
use timely::dataflow::channels::ContainerBytes;
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::{ExchangeCore, LogPuller, ParallelizationContract};
use timely::dataflow::operators::Input;
use timely::dataflow::operators::generic::Operator;
use timely::logging::TimelyLogger;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::{CommunicationConfig, WorkerConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let total_gb:          usize = parse_arg(&args, "--total-gb",          8);
    let chunk_kb:          usize = parse_arg(&args, "--chunk-kb",          256);
    let records_per_batch: usize = parse_arg(&args, "--records-per-batch", 64);
    let workers:           usize = parse_arg(&args, "--workers",           2);
    let budget_mb:         usize = parse_arg(&args, "--budget-mb",         64);
    let step_every:        usize = parse_arg(&args, "--step-every",        0);
    let rss_every_secs:    u64   = parse_arg(&args, "--rss-every-secs",    2) as u64;
    let with_spill = args.iter().any(|a| a == "--with-spill");
    let spill_dir: PathBuf = args.iter()
        .position(|a| a == "--spill-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);

    let chunk_bytes = chunk_kb << 10;
    let total_bytes = total_gb << 30;
    let total_chunks = total_bytes / chunk_bytes;
    let chunks_per_worker = total_chunks / workers;
    let budget_bytes = if with_spill { budget_mb << 20 } else { usize::MAX };

    println!("spill_cb configuration:");
    println!("  workers:            {}", workers);
    println!("  total:              {} GB ({} chunks of {} KB)", total_gb, total_chunks, chunk_kb);
    println!("  per worker:         {} chunks ({} GB)", chunks_per_worker, (chunks_per_worker * chunk_bytes) >> 30);
    println!("  records/batch:      {}", records_per_batch);
    println!("  step_every:         {} batches{}", step_every, if step_every == 0 { " (no interleave)" } else { "" });
    println!("  with_spill:         {}", with_spill);
    if with_spill {
        println!("  per-CB budget:      {} MB", budget_mb);
        println!("  spill_dir:          {}", spill_dir.display());
    }
    println!();

    // Use the Process (Typed mpsc) flavor: containers cross threads as `T`,
    // no wire serialization. `MaybeSpilled<C>`'s `ContainerBytes` impl is
    // therefore never called.
    let comm = CommunicationConfig::Process(workers);
    let (builders, others) = comm.try_build().expect("failed to build allocators");

    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let start = Instant::now();
    let sampler = std::thread::spawn(move || {
        print_rss(start, "start");
        while !stop_clone.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(rss_every_secs));
            print_rss(start, "running");
        }
        print_rss(start, "done");
    });

    let spill_dir = Arc::new(spill_dir);

    let guards = timely::execute::execute_from(builders, others, WorkerConfig::default(), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        let target = ((index + 1) % peers) as u64;

        // Each worker installs its `SpillEnv` into the per-thread cell.
        // `SpillingCB::default()` reads from this cell at pact-build time,
        // when `DrainContainerDistributor::new` constructs N per-destination
        // CBs (one per peer).
        let env = Arc::new(SpillEnv {
            budget_bytes,
            spill_dir: (*spill_dir).clone(),
            in_flight_bytes: AtomicUsize::new(0),
            spilled_bytes_total: AtomicUsize::new(0),
            spilled_containers: AtomicUsize::new(0),
        });
        SPILL_ENV.with(|c| *c.borrow_mut() = Some(Arc::clone(&env)));

        // The container traveling on the edge is `MaybeSpilled<Vec<Chunk>>`.
        // The InputHandle's CB is the lean output-port variant: a plain
        // `CapacityContainerBuilder<MaybeSpilled<_>>` that only ever holds
        // and emits `Live` containers. We don't use `send`/`PushInto`; we
        // hand pre-built `Live` batches to `send_batch`.
        let mut input = InputHandle::<u64, CapacityContainerBuilder<MaybeSpilled<Vec<Chunk>>>>::new();

        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                // Pipeline edge from input -> map. We attach a
                // `PipelineCB<SpillingCB<_>>` pact so the spill machinery
                // lives on this edge's receiving pact rather than on the
                // input's output port CB. The input's output port stays
                // lean (a plain `CapacityContainerBuilder<MaybeSpilled<_>>`),
                // matching how the exchange edge below also keeps its
                // upstream output port lean. Spill policy is uniformly a
                // per-edge concern.
                .unary::<CapacityContainerBuilder<MaybeSpilled<Vec<Chunk>>>, _, _, _>(
                    PipelineCB::<SpillingCB<Vec<Chunk>>>::new(),
                    "MapIdentity",
                    |_cap, _info| {
                        move |input, output| {
                            input.for_each(|cap, data| {
                                // Identity passthrough: forward the
                                // container as-is. The pipeline pact's
                                // CB upstream of this point already chose
                                // Live/Spilled per the spill budget.
                                output.session(&cap).give_container(data);
                            });
                        }
                    },
                )
                // The distributor-side CB is `SpillingCB<Vec<Chunk>>`. It
                // accumulates post-partition records into an inner Vec,
                // and at extract-time either emits a `Live` container or
                // serializes it to disk and emits a `Spilled` handle,
                // depending on whether `SpillEnv::in_flight_bytes` is over
                // budget.
                .sink(
                    ExchangeCore::<SpillingCB<Vec<Chunk>>, _>::new_core(move |_c: &Chunk| target),
                    "Sink",
                    {
                        let env = Arc::clone(&env);
                        let mut received_bytes: usize = 0;
                        let mut received_chunks: usize = 0;
                        let mut spilled_seen: usize = 0;
                        let mut live_seen: usize = 0;
                        let mut last_print = Instant::now();
                        move |(input, _frontier)| {
                            input.for_each(|_cap, data| {
                                match data {
                                    MaybeSpilled::Live { .. } => live_seen += 1,
                                    MaybeSpilled::Spilled { .. } => spilled_seen += 1,
                                }
                                // `DrainContainer::drain` materializes if Spilled,
                                // then yields the inner records.
                                for chunk in DrainContainer::drain(data) {
                                    received_bytes += chunk.len();
                                    received_chunks += 1;
                                }
                            });
                            if last_print.elapsed() >= Duration::from_secs(5) {
                                println!(
                                    "worker {}: received {} chunks ({} MB); live containers {}, spilled containers {}; spill-disk-bytes {} MB",
                                    index,
                                    received_chunks,
                                    received_bytes >> 20,
                                    live_seen,
                                    spilled_seen,
                                    env.spilled_bytes_total.load(Ordering::Relaxed) >> 20,
                                );
                                last_print = Instant::now();
                            }
                        }
                    },
                );
        });

        // Production: build `Live` containers of `records_per_batch` chunks
        // each and hand them to `send_batch`. If `step_every > 0`, call
        // `worker.step()` every N sent batches so the dataflow drains
        // interleaved with production — this exercises the budget cycle
        // (Live containers drain → `in_flight_bytes` decrements → later
        // batches stay Live within budget).
        let prod_start = Instant::now();
        let mut rng_state: u64 =
            0x9E37_79B9_7F4A_7C15 ^ ((index as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9));
        let mut produced: usize = 0;
        let mut batches_since_step: usize = 0;
        let mut batch: Vec<Chunk> = Vec::with_capacity(records_per_batch);
        while produced < chunks_per_worker {
            let mut buf = vec![0u8; chunk_bytes];
            let words = chunk_bytes / 8;
            let (prefix, body, _suffix) = unsafe { buf.align_to_mut::<u64>() };
            debug_assert!(prefix.is_empty());
            for w in body.iter_mut().take(words) {
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                *w = rng_state;
            }
            batch.push(buf);
            produced += 1;
            if batch.len() == records_per_batch || produced == chunks_per_worker {
                let mut container = MaybeSpilled::live(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(records_per_batch),
                ));
                input.send_batch(&mut container);
                batches_since_step += 1;
                if step_every > 0 && batches_since_step >= step_every {
                    worker.step();
                    batches_since_step = 0;
                }
            }
        }
        input.close();
        println!("worker {}: production {:.2?}", index, prod_start.elapsed());

        let drain_start = Instant::now();
        while worker.step_or_park(None) {}
        println!(
            "worker {}: drain {:.2?}; spilled containers: {}, spilled bytes: {} MB",
            index,
            drain_start.elapsed(),
            env.spilled_containers.load(Ordering::Relaxed),
            env.spilled_bytes_total.load(Ordering::Relaxed) >> 20,
        );

        index
    })
    .expect("execute_from failed");

    for r in guards.join() { let _ = r; }
    stop.store(true, Ordering::Relaxed);
    sampler.join().ok();

    let elapsed = start.elapsed();
    println!();
    println!("elapsed: {:.2?}", elapsed);
    println!("OK");
}

/// Per-record payload type.
type Chunk = Vec<u8>;

// =====================================================================
// `MaybeSpilled<C>`: a wrapper container that is either live in memory or
// parked on disk. `DrainContainer::drain` lazily slurps from disk when the
// container is `Spilled`.
// =====================================================================

#[derive(Clone)]
enum MaybeSpilled<C> {
    Live {
        c: C,
        /// Bytes the `SpillingCB` charged against `SpillEnv::in_flight_bytes`
        /// when this container was emitted. Released on `DrainContainer::drain`
        /// so the budget is a real cap, not a one-way ratchet. `0` when the
        /// container wasn't produced by a `SpillingCB` (e.g. operator output
        /// port building Live containers directly via `Default`).
        accounted_bytes: usize,
    },
    Spilled {
        handle: Arc<SpillHandle<C>>,
        /// Cached record count for `Accountable::record_count` without
        /// loading from disk.
        count: i64,
    },
}

impl<C> MaybeSpilled<C> {
    fn live(c: C) -> Self { MaybeSpilled::Live { c, accounted_bytes: 0 } }
    fn live_accounted(c: C, accounted_bytes: usize) -> Self {
        MaybeSpilled::Live { c, accounted_bytes }
    }
}

impl<C: Default> Default for MaybeSpilled<C> {
    fn default() -> Self { MaybeSpilled::live(C::default()) }
}

impl<C: Accountable> Accountable for MaybeSpilled<C> {
    fn record_count(&self) -> i64 {
        match self {
            MaybeSpilled::Live { c, .. } => c.record_count(),
            MaybeSpilled::Spilled { count, .. } => *count,
        }
    }
}

impl<C> DrainContainer for MaybeSpilled<C>
where
    C: DrainContainer + DeserializeContainer + Default + 'static,
{
    type Item<'a> = C::Item<'a> where Self: 'a;
    type DrainIter<'a> = C::DrainIter<'a> where Self: 'a;
    fn drain(&mut self) -> Self::DrainIter<'_> {
        // Materialize lazily on first drain.
        if let MaybeSpilled::Spilled { handle, .. } = self {
            let loaded = handle.load();
            *self = MaybeSpilled::live(loaded);
        }
        match self {
            MaybeSpilled::Live { c, accounted_bytes } => {
                // Release accounted bytes back to the worker budget so
                // later batches can fit Live rather than spilling. Set
                // to 0 to avoid double-release if `drain` is called
                // again on the (now-empty) container.
                let released = std::mem::take(accounted_bytes);
                if released > 0 {
                    SPILL_ENV.with(|cell| {
                        if let Some(env) = cell.borrow().as_ref() {
                            env.in_flight_bytes.fetch_sub(released, Ordering::Relaxed);
                        }
                    });
                }
                c.drain()
            }
            MaybeSpilled::Spilled { .. } => unreachable!(),
        }
    }
}

// `ContainerBytes` is required by `ParallelizationContract`'s trait
// bounds. We use the `Process` flavor, so these methods are never
// called at runtime.
impl<C: 'static> ContainerBytes for MaybeSpilled<C> {
    fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self {
        panic!("MaybeSpilled::from_bytes: this example uses CommunicationConfig::Process; wire serialization should not occur");
    }
    fn length_in_bytes(&self) -> usize {
        panic!("MaybeSpilled::length_in_bytes: this example uses CommunicationConfig::Process; wire serialization should not occur");
    }
    fn into_bytes<W: Write>(&self, _writer: &mut W) {
        panic!("MaybeSpilled::into_bytes: this example uses CommunicationConfig::Process; wire serialization should not occur");
    }
}

// =====================================================================
// Spill backend: serializes a container's records to a tempfile, returns
// a handle that can later read them back.
// =====================================================================

/// Per-worker spill configuration and accounting.
struct SpillEnv {
    /// Budget on `in_flight_bytes` (sum of Live container sizes still
    /// "in flight" — produced by `SpillingCB` but not yet consumed).
    /// `usize::MAX` disables spill.
    budget_bytes: usize,
    spill_dir: PathBuf,
    /// Sum of estimated byte sizes of `Live` containers emitted by all
    /// `SpillingCB` instances on this worker, decremented when the
    /// consumer drains them. Best-effort: we decrement on the consumer
    /// side via `LiveByteGuard`, which sits inside `Live` and decrements
    /// on `Drop`.
    in_flight_bytes: AtomicUsize,
    spilled_bytes_total: AtomicUsize,
    spilled_containers: AtomicUsize,
}

thread_local! {
    static SPILL_ENV: RefCell<Option<Arc<SpillEnv>>> = const { RefCell::new(None) };
}

fn spill_env() -> Arc<SpillEnv> {
    SPILL_ENV.with(|c| {
        c.borrow()
            .clone()
            .expect("SPILL_ENV not installed: each worker must install its env before building the dataflow")
    })
}

/// A handle to records parked on disk. Cloning the handle shares the
/// backing file via the inner `Arc`; the first `load` consumes the
/// on-disk state and caches the resulting in-memory container so
/// subsequent loads remain cheap.
struct SpillHandle<C> {
    state: Mutex<HandleState<C>>,
    bytes_on_disk: usize,
}

enum HandleState<C> {
    OnDisk { file: File },
    Loaded(C),
    Empty,
}

impl<C: DeserializeContainer + Default> SpillHandle<C> {
    fn load(&self) -> C {
        let mut state = self.state.lock().expect("spill state poisoned");
        match std::mem::replace(&mut *state, HandleState::Empty) {
            HandleState::OnDisk { mut file } => {
                file.seek(SeekFrom::Start(0)).expect("spill seek failed");
                let c = C::deserialize_from(&mut file).expect("spill deserialize failed");
                // Cache for subsequent loads (handles can be cloned).
                let cloned = clone_via_serialize(&c);
                *state = HandleState::Loaded(cloned);
                c
            }
            HandleState::Loaded(c) => {
                let cloned = clone_via_serialize(&c);
                *state = HandleState::Loaded(c);
                cloned
            }
            HandleState::Empty => unreachable!("spill handle in Empty state"),
        }
    }
}

fn clone_via_serialize<C: DeserializeContainer>(c: &C) -> C {
    // Quick container clone helper: write to an in-memory buffer, read
    // back. Used to keep `SpillHandle` clones cheap-ish for the rare
    // case where the same handle is loaded multiple times.
    let mut buf: Vec<u8> = Vec::with_capacity(c.serialized_len());
    c.serialize_into(&mut buf).expect("clone serialize failed");
    C::deserialize_from(&mut &buf[..]).expect("clone deserialize failed")
}

/// A minimal trait so `MaybeSpilled` can convert between container and
/// on-disk bytes without pulling in `serde`/`bincode`. Concrete impls
/// for the container types we ship.
trait DeserializeContainer: Sized {
    fn serialized_len(&self) -> usize;
    fn serialize_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;
    fn deserialize_from<R: Read>(reader: &mut R) -> std::io::Result<Self>;
}

// Hand-rolled framing for `Vec<Vec<u8>>`: 8-byte record count, then for
// each record an 8-byte length followed by the bytes. No padding.
impl DeserializeContainer for Vec<Vec<u8>> {
    fn serialized_len(&self) -> usize {
        let mut n = 8;
        for r in self {
            n += 8 + r.len();
        }
        n
    }
    fn serialize_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&(self.len() as u64).to_le_bytes())?;
        for r in self {
            writer.write_all(&(r.len() as u64).to_le_bytes())?;
            writer.write_all(r)?;
        }
        Ok(())
    }
    fn deserialize_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let n = u64::from_le_bytes(buf8) as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            reader.read_exact(&mut buf8)?;
            let len = u64::from_le_bytes(buf8) as usize;
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes)?;
            out.push(bytes);
        }
        Ok(out)
    }
}

// =====================================================================
// `SpillingCB<C>`: the distributor-side `ContainerBuilder`. Accumulates
// records into an inner `C`, and at `extract`/`finish` emits either a
// `Live` or a `Spilled` `MaybeSpilled<C>` depending on the worker's
// memory pressure.
// =====================================================================

struct SpillingCB<C> {
    /// Container under construction.
    current: C,
    /// Soft cap on `current`: when its accounted byte size reaches
    /// `target_bytes`, we close it out into `pending`. Tuned roughly to
    /// the size of one batch.
    target_bytes: usize,
    /// Closed containers ready for `extract`.
    pending: VecDeque<MaybeSpilled<C>>,
    /// The "out" slot returned by `&mut` from `extract`/`finish`.
    out: Option<MaybeSpilled<C>>,
    /// Shared worker spill state.
    env: Arc<SpillEnv>,
    /// Running estimate of bytes in `current` (in-memory only; doesn't
    /// touch `env.in_flight_bytes` until close-out time).
    current_bytes: usize,
}

impl Default for SpillingCB<Vec<Chunk>> {
    fn default() -> Self {
        Self {
            current: Vec::new(),
            target_bytes: 4 << 20,
            pending: VecDeque::new(),
            out: None,
            env: spill_env(),
            current_bytes: 0,
        }
    }
}

impl PushInto<Chunk> for SpillingCB<Vec<Chunk>> {
    fn push_into(&mut self, item: Chunk) {
        self.current_bytes += item.len();
        self.current.push(item);
        if self.current_bytes >= self.target_bytes {
            self.close_current();
        }
    }
}

impl SpillingCB<Vec<Chunk>> {
    fn close_current(&mut self) {
        if self.current.is_empty() {
            return;
        }
        let c = std::mem::take(&mut self.current);
        let bytes = self.current_bytes;
        self.current_bytes = 0;

        // Decide: live or spill?
        let live_before = self.env.in_flight_bytes.load(Ordering::Relaxed);
        if live_before + bytes <= self.env.budget_bytes {
            // Stay live; account the bytes. They are released when the
            // consumer drains the container (see `LiveByteGuard` below).
            self.env.in_flight_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.pending
                .push_back(MaybeSpilled::live_accounted(c, bytes));
        } else {
            // Spill: serialize to tempfile, emit handle.
            let count = c.record_count();
            let bytes_on_disk = c.serialized_len();
            let file = match tempfile::tempfile_in(&self.env.spill_dir) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("spill tempfile failed: {}; emitting Live", e);
                    self.env.in_flight_bytes.fetch_add(bytes, Ordering::Relaxed);
                    self.pending.push_back(MaybeSpilled::live_accounted(c, bytes));
                    return;
                }
            };
            let mut writer = BufWriter::with_capacity(1 << 20, file);
            if let Err(e) = c.serialize_into(&mut writer) {
                eprintln!("spill write failed: {}; emitting Live", e);
                self.env.in_flight_bytes.fetch_add(bytes, Ordering::Relaxed);
                self.pending.push_back(MaybeSpilled::live_accounted(c, bytes));
                return;
            }
            let file = match writer.into_inner() {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("spill flush failed: {}; emitting Live", e.error());
                    self.env.in_flight_bytes.fetch_add(bytes, Ordering::Relaxed);
                    self.pending.push_back(MaybeSpilled::live_accounted(c, bytes));
                    return;
                }
            };
            let handle = Arc::new(SpillHandle {
                state: Mutex::new(HandleState::OnDisk { file }),
                bytes_on_disk,
            });
            self.env
                .spilled_bytes_total
                .fetch_add(handle.bytes_on_disk, Ordering::Relaxed);
            self.env.spilled_containers.fetch_add(1, Ordering::Relaxed);
            self.pending
                .push_back(MaybeSpilled::Spilled { handle, count });
        }
    }
}

impl ContainerBuilder for SpillingCB<Vec<Chunk>> {
    type Container = MaybeSpilled<Vec<Chunk>>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if self.current_bytes >= self.target_bytes {
            self.close_current();
        }
        if let Some(c) = self.pending.pop_front() {
            self.out = Some(c);
            self.out.as_mut()
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            self.close_current();
        }
        self.out = self.pending.pop_front();
        self.out.as_mut()
    }
}

// Record-count preservation: `SpillingCB` outputs every record it
// receives, just sometimes packed in a `Spilled` wrapper that still
// reports the same `record_count`. Safe to mark.
impl LengthPreservingContainerBuilder for SpillingCB<Vec<Chunk>> {}

// =====================================================================
// Live-bytes accounting: when a `Live(c)` is drained on the consumer
// side, we release its bytes from `env.in_flight_bytes`. We do this from
// `DrainContainer::drain`, since the drain is exactly the moment the
// consumer takes responsibility for the records. The chunk byte size
// is recomputed at release time.
// =====================================================================

// Wrap the `Live` drain in a guard that decrements on drop. We change
// the `DrainContainer` impl above to route through this guard.
//
// We could thread bytes-released-on-drain through the `MaybeSpilled`
// itself; for clarity, we recompute on release here.
//
// (Implementation note: the simple approach above already calls
// `Live(c).drain()`; we attach the release inside the `MaybeSpilled`
// drain to keep accounting local. The implementation lives in the
// `DrainContainer` impl earlier in the file but we keep the size
// computation here for clarity.)
fn _accounting_doc() {} // marker — actual logic is inline above

// =====================================================================
// `PipelineCB<CB>`: a parallelization contract for pipeline (intra-worker)
// edges that runs a `ContainerBuilder` on the producer side of the local
// channel. Symmetric with how `ExchangeCore<CB, _>` runs a per-destination
// CB inside its distributor. Downstream-implementable: uses only public
// `ParallelizationContract` machinery and `Worker::pipeline`.
//
// With this pact, spill policy lives uniformly in the receiving pact for
// both pipeline and exchange edges; producer output ports never need to
// know whether the next edge spills.
// =====================================================================

/// Pipeline pact that runs `CB` on the producer side.
pub struct PipelineCB<CB>(PhantomData<CB>);

impl<CB> PipelineCB<CB> {
    pub fn new() -> Self { Self(PhantomData) }
}

impl<T, C, CB> ParallelizationContract<T, C> for PipelineCB<CB>
where
    T: Timestamp,
    C: Accountable + DrainContainer + Default + Clone + 'static,
    CB: ContainerBuilder<Container = C>
        + for<'a> PushInto<<C as DrainContainer>::Item<'a>>
        + 'static,
{
    type Pusher = CBPusher<T, CB>;
    type Puller = LogPuller<Box<dyn Pull<Message<T, C>>>>;

    fn connect(
        self,
        worker: &Worker,
        identifier: usize,
        address: Rc<[usize]>,
        logging: Option<TimelyLogger>,
    ) -> (Self::Pusher, Self::Puller) {
        let (pusher, puller) = worker.pipeline::<Message<T, C>>(identifier, address);
        (
            CBPusher::new(Box::new(pusher)),
            LogPuller::new(Box::new(puller), worker.index(), identifier, logging),
        )
    }
}

/// The pipeline-pact pusher: drains incoming records and re-batches them
/// through `CB`, then forwards built containers to the underlying local
/// channel pusher.
pub struct CBPusher<T, CB: ContainerBuilder> {
    inner: Box<dyn Push<Message<T, CB::Container>>>,
    builder: CB,
    current_time: Option<T>,
}

impl<T, CB: ContainerBuilder + Default> CBPusher<T, CB> {
    fn new(inner: Box<dyn Push<Message<T, CB::Container>>>) -> Self {
        Self {
            inner,
            builder: CB::default(),
            current_time: None,
        }
    }
}

impl<T, CB> Push<Message<T, CB::Container>> for CBPusher<T, CB>
where
    T: Timestamp,
    CB: ContainerBuilder<Container: DrainContainer + Default>
        + for<'a> PushInto<<CB::Container as DrainContainer>::Item<'a>>,
{
    fn push(&mut self, msg: &mut Option<Message<T, CB::Container>>) {
        // Split-borrow helpers as local closures avoid &mut self aliasing
        // between `self.builder` and `self.inner`.
        let Self { inner, builder, current_time } = self;

        let mut emit = |time: T, built: &mut CB::Container| {
            let data = std::mem::take(built);
            let mut bundle = Some(Message::new(time, data));
            inner.push(&mut bundle);
        };

        match msg.take() {
            Some(mut message) => {
                if let Some(cur) = current_time.as_ref() {
                    if cur != &message.time {
                        let prev = current_time.take().unwrap();
                        while let Some(built) = builder.finish() {
                            emit(prev.clone(), built);
                        }
                    }
                }
                *current_time = Some(message.time.clone());

                let time = message.time.clone();
                for item in message.data.drain() {
                    builder.push_into(item);
                    while let Some(built) = builder.extract() {
                        emit(time.clone(), built);
                    }
                }
            }
            None => {
                if let Some(t) = current_time.take() {
                    while let Some(built) = builder.finish() {
                        emit(t.clone(), built);
                    }
                }
                // Mirror Exchange::push: after draining built containers,
                // ask the CB to release its resources, then propagate the
                // "done" signal to the inner pusher.
                builder.relax();
                inner.push(&mut None);
            }
        }
    }
}

// =====================================================================
// CLI plumbing and RSS sampler (lifted from `spill_compare.rs`).
// =====================================================================

fn parse_arg(args: &[String], flag: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn get_rss_kb() -> Option<u64> {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

fn print_rss(start: Instant, label: &str) {
    match get_rss_kb() {
        Some(kb) => println!(
            "[t={:>6.1}s  RSS {:>8} KB / {:>6} MB]  {}",
            start.elapsed().as_secs_f64(),
            kb,
            kb / 1024,
            label,
        ),
        None => println!(
            "[t={:>6.1}s  RSS unavailable]  {}",
            start.elapsed().as_secs_f64(),
            label,
        ),
    }
}
