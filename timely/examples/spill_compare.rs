//! Pushes a configurable volume (default 50 GB) of `Vec<u8>` chunks through
//! a timely `Exchange`, optionally with the spilling `MergeQueue` policy
//! installed. Compare RSS and wall-clock time:
//!
//!   # spill enabled (file-backed): bounded RSS, disk traffic to --spill-dir
//!   cargo run --release --example spill_compare -- --with-spill -w 2
//!
//!   # spill disabled: OS pages or OOMs once the queue exceeds RAM
//!   cargo run --release --example spill_compare -- -w 2
//!
//! Spill engages on the zero-copy `MergeQueue` path, so this example always
//! uses `CommunicationConfig::ProcessBinary(workers)`. Each worker injects
//! `--total-gb / workers` of data and routes every record to peer
//! `(index + 1) % workers`, forcing all bytes across a MergeQueue.
//!
//! Options:
//!   --total-gb N         Total cluster-wide data (default: 50)
//!   --chunk-kb N         Size of each Vec<u8> sent (default: 256)
//!   --workers N          Number of worker threads (default: 2)
//!   --threshold-mb N     Spill threshold (default: 256)
//!   --head-reserve-mb N  Head reserve / prefetch budget (default: 64)
//!   --spill-dir PATH     Directory for tempfiles (default: std::env::temp_dir())
//!   --with-spill         Install the file-backed spill policy
//!   --rss-every-secs N   RSS sampling cadence (default: 2)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use timely::CommunicationConfig;
use timely::WorkerConfig;
use timely::communication::initialize::Hooks;
use timely::dataflow::InputHandle;
use timely::dataflow::operators::Input;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::Exchange;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let total_gb:        usize = parse_arg(&args, "--total-gb",        50);
    let chunk_kb:        usize = parse_arg(&args, "--chunk-kb",        256);
    let workers:         usize = parse_arg(&args, "--workers",         2);
    let threshold_mb:    usize = parse_arg(&args, "--threshold-mb",    256);
    let head_reserve_mb: usize = parse_arg(&args, "--head-reserve-mb", 64);
    let rss_every_secs:  u64   = parse_arg(&args, "--rss-every-secs",  2) as u64;
    let with_spill = args.iter().any(|a| a == "--with-spill");
    let spill_dir: std::path::PathBuf = args.iter()
        .position(|a| a == "--spill-dir")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);

    let chunk_bytes = chunk_kb << 10;
    let total_bytes = total_gb << 30;
    let total_chunks = total_bytes / chunk_bytes;
    let chunks_per_worker = total_chunks / workers;

    println!("spill_compare configuration:");
    println!("  workers:         {}", workers);
    println!("  total:           {} GB ({} chunks of {} KB)", total_gb, total_chunks, chunk_kb);
    println!("  per worker:      {} chunks ({} GB)", chunks_per_worker, (chunks_per_worker * chunk_bytes) >> 30);
    println!("  with_spill:      {}", with_spill);
    if with_spill {
        println!("  threshold:       {} MB", threshold_mb);
        println!("  head_reserve:    {} MB", head_reserve_mb);
        println!("  spill_dir:       {}", spill_dir.display());
    }
    println!();

    // Build hooks. If --with-spill, install a file-backed policy factory.
    let mut hooks = Hooks::default();
    if with_spill {
        let threshold_bytes = threshold_mb << 20;
        let head_reserve = head_reserve_mb << 20;
        let dir = spill_dir.clone();
        hooks.spill = Some(Arc::new(move || {
            use timely::communication::allocator::zero_copy::spill::{
                SpillPolicy, Threshold, PrefetchPolicy,
            };
            let strategy = Box::new(file_spill::FileSpillStrategy::new(dir.clone()));
            let mut tp = Threshold::new(strategy);
            tp.threshold_bytes = threshold_bytes;
            tp.head_reserve_bytes = head_reserve;
            let writer: Box<dyn SpillPolicy> = Box::new(tp);
            let reader: Box<dyn SpillPolicy> = Box::new(PrefetchPolicy::new(head_reserve));
            (writer, reader)
        }));
    }

    let comm = CommunicationConfig::ProcessBinary(workers);
    let (builders, others) = comm.try_build_with(hooks).expect("failed to build allocators");

    // RSS sampler thread, stopped when the workers finish.
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

    let guards = timely::execute::execute_from(builders, others, WorkerConfig::default(), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        let target = ((index + 1) % peers) as u64;

        let mut input = InputHandle::<u64, timely::container::CapacityContainerBuilder<Vec<serde_bytes::ByteBuf>>>::new();

        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .sink(Exchange::new(move |_v: &serde_bytes::ByteBuf| target), "Sink", {
                     let mut received_bytes: usize = 0;
                     let mut received_chunks: usize = 0;
                     let mut last_print = Instant::now();
                     move |(input, _frontier)| {
                         input.for_each(|_cap, data| {
                             for v in data.drain(..) {
                                 received_bytes += v.len();
                                 received_chunks += 1;
                             }
                             if last_print.elapsed() >= Duration::from_secs(5) {
                                 println!("worker {}: received {} chunks ({} MB)",
                                     index, received_chunks, received_bytes >> 20);
                                 last_print = Instant::now();
                             }
                         });
                     }
                 });
        });

        // Production: each worker pushes its share into the input handle in
        // one shot, with no `step` calls. Then closes the input and lets
        // the framework drain.
        let prod_start = Instant::now();
        // xorshift64* keeps the bytes incompressible so macOS's compressed
        // memory can't squash duplicate pages.
        let mut rng_state: u64 = 0x9E37_79B9_7F4A_7C15
            ^ ((index as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9));
        for _ in 0..chunks_per_worker {
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
            input.send(serde_bytes::ByteBuf::from(buf));
        }
        input.close();
        println!("worker {}: production {:.2?}", index, prod_start.elapsed());

        let drain_start = Instant::now();
        while worker.step_or_park(None) { }
        println!("worker {}: drain {:.2?}", index, drain_start.elapsed());

        index
    }).expect("execute_from failed");

    for r in guards.join() { let _ = r; }
    stop.store(true, Ordering::Relaxed);
    sampler.join().ok();

    let elapsed = start.elapsed();
    println!();
    println!("elapsed: {:.2?}", elapsed);
    println!("OK");
}

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
        Some(kb) => println!("[t={:>6.1}s  RSS {:>8} KB / {:>6} MB]  {}",
            start.elapsed().as_secs_f64(), kb, kb / 1024, label),
        None     => println!("[t={:>6.1}s  RSS unavailable]  {}", start.elapsed().as_secs_f64(), label),
    }
}

/// File-backed BytesSpill implementation; mirrors `communication/examples/spill_stress.rs`.
mod file_spill {
    use std::fs::File;
    use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use timely::bytes::arc::{Bytes, BytesMut};
    use timely::communication::allocator::zero_copy::spill::{BytesSpill, BytesFetch};

    pub struct FileSpillStrategy { dir: PathBuf }

    impl FileSpillStrategy {
        pub fn new(dir: PathBuf) -> Self { FileSpillStrategy { dir } }
    }

    impl BytesSpill for FileSpillStrategy {
        fn spill(&mut self, chunks: &mut Vec<Bytes>, handles: &mut Vec<Box<dyn BytesFetch>>) {
            if chunks.is_empty() { return; }
            let raw = match tempfile::tempfile_in(&self.dir) {
                Ok(f) => f,
                Err(e) => { eprintln!("file spill failed: {}", e); return; }
            };
            let mut writer = BufWriter::with_capacity(4 << 20, raw);
            let mut lens = Vec::with_capacity(chunks.len());
            for chunk in chunks.iter() {
                if let Err(e) = writer.write_all(&chunk[..]) {
                    eprintln!("file spill write failed: {}", e);
                    return;
                }
                lens.push(chunk.len());
            }
            let file = match writer.into_inner() {
                Ok(f) => f,
                Err(e) => { eprintln!("file spill flush failed: {}", e); return; }
            };
            chunks.clear();
            let state = Arc::new(Mutex::new(FileState::OnDisk { file, lens: lens.clone() }));
            handles.extend((0..lens.len()).map(|i| Box::new(ChunkHandle {
                state: Arc::clone(&state), index: i,
            }) as Box<dyn BytesFetch>));
        }
    }

    enum FileState {
        OnDisk { file: File, lens: Vec<usize> },
        Slurped { chunks: Vec<Bytes> },
        Placeholder,
    }

    struct ChunkHandle { state: Arc<Mutex<FileState>>, index: usize }

    impl BytesFetch for ChunkHandle {
        fn fetch(self: Box<Self>) -> Result<Vec<Bytes>, Box<dyn BytesFetch>> {
            let mut state = self.state.lock().expect("spill state poisoned");
            if matches!(*state, FileState::OnDisk { .. }) {
                let (mut file, lens) = match std::mem::replace(&mut *state, FileState::Placeholder) {
                    FileState::OnDisk { file, lens } => (file, lens),
                    _ => unreachable!(),
                };
                if let Err(e) = file.seek(SeekFrom::Start(0)) {
                    eprintln!("spill fetch seek failed: {}", e);
                    *state = FileState::OnDisk { file, lens };
                    drop(state);
                    return Err(self);
                }
                let mut chunks = Vec::with_capacity(lens.len());
                for &len in &lens {
                    let mut data = vec![0u8; len];
                    if let Err(e) = file.read_exact(&mut data) {
                        eprintln!("spill fetch read failed: {}", e);
                        *state = FileState::OnDisk { file, lens };
                        drop(state);
                        return Err(self);
                    }
                    chunks.push(BytesMut::from(data).freeze());
                }
                *state = FileState::Slurped { chunks };
            }
            let result = match &*state {
                FileState::Slurped { chunks } => Ok(vec![chunks[self.index].clone()]),
                _ => unreachable!(),
            };
            drop(state);
            result
        }
    }
}
