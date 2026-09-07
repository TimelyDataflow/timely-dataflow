//! Measures steady-state allocations in typed and binary exchange paths.
//!
//! Run with, for example:
//! `cargo run --release --example transport_alloc -- binary columnar 4 100 10000`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use timely::Accountable;
use timely::container::columnar::{ColumnarBuilder, ColumnarContainer};
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};
use timely::dataflow::operators::{Exchange, InspectCore, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNTS: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
static ALLOCATION_BYTES: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
static RECEIVED_CONTAINERS: AtomicUsize = AtomicUsize::new(0);
static BYTE_BACKED_CONTAINERS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: Delegates the allocation with the unchanged layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Delegates the deallocation with the original pointer and layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
        // SAFETY: Delegates the reallocation with the original allocation metadata.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[inline]
fn record_allocation(size: usize) {
    if !MEASURING.load(Ordering::Relaxed) {
        return;
    }
    let bucket = match size {
        0..=256 => 0,
        257..=4_096 => 1,
        4_097..=65_536 => 2,
        65_537..=262_144 => 3,
        262_145..=1_048_576 => 4,
        _ => 5,
    };
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
    ALLOCATION_COUNTS[bucket].fetch_add(1, Ordering::Relaxed);
    ALLOCATION_BYTES[bucket].fetch_add(size, Ordering::Relaxed);
}

fn reset_allocations() {
    ALLOCATIONS.store(0, Ordering::SeqCst);
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    for counter in ALLOCATION_COUNTS.iter().chain(ALLOCATION_BYTES.iter()) {
        counter.store(0, Ordering::SeqCst);
    }
}

fn allocation_histogram() -> String {
    let labels = ["0-256", "257-4K", "4K-64K", "64K-256K", "256K-1M", ">1M"];
    labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            format!(
                "{label}:{}:{}",
                ALLOCATION_COUNTS[index].load(Ordering::SeqCst),
                ALLOCATION_BYTES[index].load(Ordering::SeqCst),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Clone, Serialize, Deserialize, columnar::Columnar)]
struct Record {
    key: u64,
    payload: [u64; 7],
}

#[derive(Clone, Copy)]
enum Transport {
    Typed,
    Binary,
}

impl Transport {
    fn config(self, workers: usize) -> timely::CommunicationConfig {
        match self {
            Transport::Typed => timely::CommunicationConfig::Process(workers),
            Transport::Binary => timely::CommunicationConfig::ProcessBinary(workers),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Transport::Typed => "typed",
            Transport::Binary => "binary",
        }
    }
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 5 {
        eprintln!(
            "usage: transport_alloc <typed|binary> <vec|columnar|columnar-builder> <workers> <rounds> <records-per-round>"
        );
        std::process::exit(2);
    }

    let transport = match args[0].as_str() {
        "typed" => Transport::Typed,
        "binary" => Transport::Binary,
        other => panic!("unknown transport: {other}"),
    };
    let workers = args[2].parse().expect("workers must be an integer");
    let rounds = args[3].parse().expect("rounds must be an integer");
    let records = args[4]
        .parse()
        .expect("records-per-round must be an integer");

    match args[1].as_str() {
        "vec" => run_vec(transport, workers, rounds, records),
        "columnar" => run_columnar(transport, workers, rounds, records),
        "columnar-builder" => run_columnar_builder(rounds, records),
        other => panic!("unknown container: {other}"),
    }
}

fn run_columnar_builder(rounds: usize, records: usize) {
    type Columns = <Record as columnar::Columnar>::Container;
    let mut builder = ColumnarBuilder::<Columns>::default();
    for record in 0..records {
        let key = record as u64;
        let payload = [key; 7];
        builder.push_into(RecordReference {
            key: &key,
            payload: &payload,
        });
    }
    while let Some(container) = builder.finish() {
        black_box(container.borrow());
    }
    builder.relax();
    reset_allocations();
    MEASURING.store(true, Ordering::SeqCst);
    let start = Instant::now();
    for round in 0..rounds {
        for record in 0..records {
            let key = (round * records + record) as u64;
            let payload = [key; 7];
            builder.push_into(RecordReference {
                key: &key,
                payload: &payload,
            });
        }
        while let Some(container) = builder.finish() {
            black_box(container.borrow());
        }
        builder.relax();
    }
    let count = rounds * records;
    let elapsed = start.elapsed().as_secs_f64();
    MEASURING.store(false, Ordering::SeqCst);
    let allocations = ALLOCATIONS.load(Ordering::SeqCst);
    let bytes = ALLOCATED_BYTES.load(Ordering::SeqCst);
    println!(
        "transport=none container=columnar-builder workers=1 records={} seconds={elapsed:.6} records_per_second={:.0} allocations={} allocated_bytes={} bytes_per_record={:.3} allocation_histogram={}",
        count,
        count as f64 / elapsed,
        allocations,
        bytes,
        bytes as f64 / count as f64,
        allocation_histogram(),
    );
}

fn run_vec(transport: Transport, workers: usize, rounds: usize, records: usize) {
    run(
        transport,
        "vec",
        workers,
        rounds,
        records,
        move |worker, shared| {
            let mut input = InputHandle::<usize, CapacityContainerBuilder<Vec<Record>>>::new();
            let mut probe = ProbeHandle::new();
            let seen = Arc::clone(&shared.seen);

            worker.dataflow::<usize, _, _>(|scope| {
                input
                    .to_stream(scope)
                    .exchange(|record| record.key)
                    .inspect_container(move |event| {
                        if let Ok((_time, data)) = event {
                            seen.fetch_add(data.len(), Ordering::Relaxed);
                            black_box(data);
                        }
                    })
                    .probe_with(&mut probe);
            });

            for record in 0..records {
                let key = (record + worker.index()) as u64;
                input.send(Record {
                    key,
                    payload: [key; 7],
                });
            }
            input.advance_to(1);
            while probe.less_than(input.time()) {
                worker.step();
            }
            shared.start_measurement(worker.index());
            let start = Instant::now();
            for round in 0..rounds {
                for record in 0..records {
                    let key = (round * records + record + worker.index()) as u64;
                    input.send(Record {
                        key,
                        payload: [key; 7],
                    });
                }
                input.advance_to(round + 2);
                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
            shared.finish_measurement(start.elapsed(), worker.index());
        },
    );
}

fn run_columnar(transport: Transport, workers: usize, rounds: usize, records: usize) {
    type Columns = <Record as columnar::Columnar>::Container;
    type Container = ColumnarContainer<Columns>;

    run(
        transport,
        "columnar",
        workers,
        rounds,
        records,
        move |worker, shared| {
            let mut input = InputHandle::<usize, ColumnarBuilder<Columns>>::new_with_builder();
            let mut probe = ProbeHandle::new();
            let seen = Arc::clone(&shared.seen);

            worker.dataflow::<usize, _, _>(|scope| {
                input
                    .to_stream(scope)
                    .exchange(|record| *record.key)
                    .inspect_container(move |event| {
                        if let Ok((_time, data)) = event {
                            seen.fetch_add(data.record_count() as usize, Ordering::Relaxed);
                            RECEIVED_CONTAINERS.fetch_add(1, Ordering::Relaxed);
                            BYTE_BACKED_CONTAINERS
                                .fetch_add(usize::from(data.is_bytes()), Ordering::Relaxed);
                            black_box(data.borrow());
                        }
                    })
                    .probe_with(&mut probe);
            });

            for record in 0..records {
                let key = (record + worker.index()) as u64;
                let payload = [key; 7];
                input.send(RecordReference {
                    key: &key,
                    payload: &payload,
                });
            }
            input.advance_to(1);
            while probe.less_than(input.time()) {
                worker.step();
            }
            shared.start_measurement(worker.index());
            let start = Instant::now();
            for round in 0..rounds {
                for record in 0..records {
                    let key = (round * records + record + worker.index()) as u64;
                    let payload = [key; 7];
                    input.send(RecordReference {
                        key: &key,
                        payload: &payload,
                    });
                }
                input.advance_to(round + 2);
                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
            shared.finish_measurement(start.elapsed(), worker.index());
        },
    );

    // Keep the alias checked as part of the example; it also documents the
    // concrete container users select for columnar streams.
    let _: Option<Container> = None;
}

struct Shared {
    barrier: Barrier,
    elapsed_ns: AtomicU64,
    seen: Arc<AtomicUsize>,
    transport: &'static str,
    container: &'static str,
    workers: usize,
    expected: usize,
}

impl Shared {
    fn start_measurement(&self, worker: usize) {
        self.barrier.wait();
        if worker == 0 {
            reset_allocations();
            self.seen.store(0, Ordering::SeqCst);
            MEASURING.store(true, Ordering::SeqCst);
            RECEIVED_CONTAINERS.store(0, Ordering::SeqCst);
            BYTE_BACKED_CONTAINERS.store(0, Ordering::SeqCst);
        }
        self.barrier.wait();
    }

    fn finish_measurement(&self, elapsed: std::time::Duration, worker: usize) {
        self.elapsed_ns
            .fetch_max(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.barrier.wait();
        if worker == 0 {
            MEASURING.store(false, Ordering::SeqCst);
            let seen = self.seen.load(Ordering::Relaxed);
            assert_eq!(seen, self.expected);
            let allocations = ALLOCATIONS.load(Ordering::SeqCst);
            let bytes = ALLOCATED_BYTES.load(Ordering::SeqCst);
            let received_containers = RECEIVED_CONTAINERS.load(Ordering::SeqCst);
            let byte_backed_containers = BYTE_BACKED_CONTAINERS.load(Ordering::SeqCst);
            let seconds = self.elapsed_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
            println!(
                "transport={} container={} workers={} records={} seconds={seconds:.6} records_per_second={:.0} allocations={} allocated_bytes={} bytes_per_record={:.3} received_containers={} byte_backed_containers={} allocation_histogram={}",
                self.transport,
                self.container,
                self.workers,
                seen,
                seen as f64 / seconds,
                allocations,
                bytes,
                bytes as f64 / seen as f64,
                received_containers,
                byte_backed_containers,
                allocation_histogram(),
            );
        }
        self.barrier.wait();
    }
}

fn run<F>(
    transport: Transport,
    container: &'static str,
    workers: usize,
    rounds: usize,
    records: usize,
    logic: F,
) where
    F: Fn(&mut timely::worker::Worker, &Arc<Shared>) + Send + Sync + 'static,
{
    let expected = workers * rounds * records;
    let shared = Arc::new(Shared {
        barrier: Barrier::new(workers),
        elapsed_ns: AtomicU64::new(0),
        seen: Arc::new(AtomicUsize::new(0)),
        transport: transport.name(),
        container,
        workers,
        expected,
    });
    let worker_shared = Arc::clone(&shared);
    let config = timely::Config {
        communication: transport.config(workers),
        worker: timely::WorkerConfig::default(),
    };

    timely::execute(config, move |worker| logic(worker, &worker_shared))
        .expect("timely execution should initialize");
}
