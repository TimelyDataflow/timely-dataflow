//! Pushes a configurable volume of data through a MergeQueue with spill
//! enabled, printing RSS at intervals. Useful for verifying that the spill
//! mechanism keeps memory bounded under load.
//!
//! Also serves as a reference implementation of a file-backed `BytesSpill`
//! strategy, demonstrating how to implement the spill traits against a
//! concrete storage backend.
//!
//! Usage:
//!   cargo run --example spill_stress -p timely_communication -- [OPTIONS]
//!
//! Options:
//!   --total-mb N        Total data to push (default: 512)
//!   --chunk-kb N        Size of each pushed Bytes (default: 256)
//!   --threshold-mb N    Spill threshold (default: 32)
//!   --head-reserve-mb N Head reserve / prefetch budget (default: 16)
//!   --spill-dir PATH    Directory for tempfiles (default: std::env::temp_dir())
//!   --drain-every N     Drain after every N pushes (default: 0 = push all then drain)

use std::time::Instant;

use timely_bytes::arc::BytesMut;
use timely_communication::allocator::zero_copy::bytes_exchange::{MergeQueue, BytesPush, BytesPull};
use timely_communication::allocator::zero_copy::spill::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let total_mb:       usize = parse_arg(&args, "--total-mb",       512);
    let chunk_kb:       usize = parse_arg(&args, "--chunk-kb",       256);
    let threshold_mb:   usize = parse_arg(&args, "--threshold-mb",   32);
    let head_reserve_mb:usize = parse_arg(&args, "--head-reserve-mb",16);
    let drain_every:    usize = parse_arg(&args, "--drain-every",    0);
    let spill_dir: std::path::PathBuf = args.iter()
        .position(|a| a == "--spill-dir")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);

    let total_bytes     = total_mb << 20;
    let chunk_bytes     = chunk_kb << 10;
    let threshold_bytes = threshold_mb << 20;
    let head_reserve    = head_reserve_mb << 20;
    let num_chunks      = total_bytes / chunk_bytes;

    println!("spill_stress configuration:");
    println!("  total:        {} MB ({} chunks of {} KB)", total_mb, num_chunks, chunk_kb);
    println!("  threshold:    {} MB", threshold_mb);
    println!("  head_reserve: {} MB", head_reserve_mb);
    println!("  drain_every:  {}", if drain_every == 0 { "push-all-then-drain".to_string() } else { format!("{}", drain_every) });
    println!("  spill_dir:    {}", spill_dir.display());
    println!();

    // Build writer + reader pair.
    let strategy: Box<dyn BytesSpill> =
        Box::new(file_spill::FileSpillStrategy::new(spill_dir));
    let mut tp = Threshold::new(strategy);
    tp.threshold_bytes = threshold_bytes;
    tp.head_reserve_bytes = head_reserve;
    let writer_policy: Box<dyn SpillPolicy> = Box::new(tp);
    let reader_policy: Box<dyn SpillPolicy> = Box::new(PrefetchPolicy::new(head_reserve));

    let buzzer = timely_communication::buzzer::Buzzer::default();
    let (mut writer, mut reader) = MergeQueue::new_pair(buzzer, Some(writer_policy), Some(reader_policy));

    let start = Instant::now();
    let mut pushed = 0usize;
    let mut drained_bytes = 0usize;
    let mut drain_buf: Vec<timely_bytes::arc::Bytes> = Vec::new();

    print_rss("start");

    // Push phase (optionally interleaved with drains).
    for i in 0..num_chunks {
        let data = vec![(i % 251) as u8; chunk_bytes];
        let bytes = BytesMut::from(data).freeze();
        writer.extend(Some(bytes));
        pushed += chunk_bytes;

        if drain_every > 0 && (i + 1) % drain_every == 0 {
            reader.drain_into(&mut drain_buf);
            for b in drain_buf.drain(..) { drained_bytes += b.len(); }
        }

        if (i + 1) % (num_chunks / 8).max(1) == 0 {
            print_rss(&format!("pushed {}/{} MB", pushed >> 20, total_mb));
        }
    }

    print_rss("push complete, starting drain");

    // Drain phase.
    loop {
        let before = drain_buf.len();
        reader.drain_into(&mut drain_buf);
        if drain_buf.len() == before { break; }
        for b in drain_buf.drain(..) { drained_bytes += b.len(); }

        if drained_bytes % (total_bytes / 8).max(1) < chunk_bytes {
            print_rss(&format!("drained {}/{} MB", drained_bytes >> 20, total_mb));
        }
    }

    let elapsed = start.elapsed();
    print_rss("drain complete");
    println!();
    println!("pushed:  {} MB", pushed >> 20);
    println!("drained: {} MB", drained_bytes >> 20);
    println!("elapsed: {:.2?}", elapsed);

    assert_eq!(pushed, drained_bytes, "data loss: pushed {} but drained {}", pushed, drained_bytes);
    println!("OK — all bytes recovered.");
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
    let text = String::from_utf8_lossy(&output.stdout);
    text.trim().parse().ok()
}

fn print_rss(label: &str) {
    match get_rss_kb() {
        Some(kb) => println!("[RSS {:>8} KB / {:>6} MB]  {}", kb, kb / 1024, label),
        None     => println!("[RSS unavailable]  {}", label),
    }
}

/// File-backed BytesSpill implementation.
///
/// One tempfile per spill batch. Writes chunks sequentially; reads by
/// slurping the whole file on first fetch. Reference implementation for
/// anyone writing their own BytesSpill backend.
mod file_spill {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use timely_bytes::arc::{Bytes, BytesMut};
    use timely_communication::allocator::zero_copy::spill::{BytesSpill, BytesFetch};

    pub struct FileSpillStrategy {
        dir: PathBuf,
    }

    impl FileSpillStrategy {
        pub fn new(dir: PathBuf) -> Self {
            FileSpillStrategy { dir }
        }
    }

    impl BytesSpill for FileSpillStrategy {
        fn spill(&mut self, chunks: &mut Vec<Bytes>, handles: &mut Vec<Box<dyn BytesFetch>>) {
            if chunks.is_empty() { return; }
            let mut file = match tempfile::tempfile_in(&self.dir) {
                Ok(f) => f,
                Err(e) => { eprintln!("timely: file spill failed: {}", e); return; }
            };
            let mut lens = Vec::with_capacity(chunks.len());
            for chunk in chunks.iter() {
                if let Err(e) = file.write_all(&chunk[..]) {
                    eprintln!("timely: file spill write failed: {}", e);
                    // Leave remaining chunks for the caller to retain.
                    return;
                }
                lens.push(chunk.len());
            }
            // All writes succeeded; drain chunks and produce handles.
            chunks.clear();
            let state = Arc::new(Mutex::new(FileState::OnDisk { file, lens: lens.clone() }));
            handles.extend((0..lens.len())
                .map(|i| Box::new(ChunkHandle {
                    state: Arc::clone(&state),
                    index: i,
                }) as Box<dyn BytesFetch>));
        }
    }

    enum FileState {
        OnDisk { file: File, lens: Vec<usize> },
        Slurped { chunks: Vec<Bytes> },
        Placeholder,
    }

    struct ChunkHandle {
        state: Arc<Mutex<FileState>>,
        index: usize,
    }

    impl BytesFetch for ChunkHandle {
        fn fetch(self: Box<Self>) -> Result<Vec<Bytes>, Box<dyn BytesFetch>> {
            let mut state = self.state.lock().expect("spill state poisoned");

            if matches!(*state, FileState::OnDisk { .. }) {
                let (mut file, lens) = match std::mem::replace(&mut *state, FileState::Placeholder) {
                    FileState::OnDisk { file, lens } => (file, lens),
                    _ => unreachable!(),
                };
                if let Err(e) = file.seek(SeekFrom::Start(0)) {
                    eprintln!("spill fetch: seek failed: {}", e);
                    *state = FileState::OnDisk { file, lens };
                    drop(state);
                    return Err(self);
                }
                let mut chunks = Vec::with_capacity(lens.len());
                for &len in &lens {
                    let mut data = vec![0u8; len];
                    if let Err(e) = file.read_exact(&mut data) {
                        eprintln!("spill fetch: read failed: {}", e);
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
                _ => unreachable!("state should be Slurped after slurp transition"),
            };
            drop(state);
            result
        }
    }
}
