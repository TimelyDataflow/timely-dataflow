# Allocation audit tracker

Look for: memory leaks, unbounded growth, unnecessary allocations in hot paths,
missing capacity hints, redundant clones, Vec/String allocations that could be avoided.

## Crates and areas

### `bytes/` (1 file) — DONE
* [x] `bytes/src/lib.rs` — clean (Arc-based sharing is efficient)

### `container/` (1 file) — DONE
* [x] `container/src/lib.rs` — 3 findings (VecDeque never shrinks, ensure_capacity math, clone-by-reference)

### `logging/` (1 file) — DONE
* [x] `logging/src/lib.rs` — 2 findings (Rc cycle hazard in Logger API, Instant::elapsed per log call)

### `communication/` (20 files) — DONE
* [x] `communication/src/lib.rs` — clean
* [x] `communication/src/initialize.rs` — 2 findings (BytesRefill double indirection, missing capacity hints)
* [x] `communication/src/buzzer.rs` — clean
* [x] `communication/src/logging.rs` — clean
* [x] `communication/src/networking.rs` — 1 finding (unconditional println in polling loop)
* [x] `communication/src/allocator/mod.rs` — clean
* [x] `communication/src/allocator/process.rs` — 3 findings (quadratic sender clones, pusher clones, unbounded receive drain)
* [x] `communication/src/allocator/counters.rs` — 2 findings (per-message event push, unconditional buzzing)
* [x] `communication/src/allocator/generic.rs` — clean
* [x] `communication/src/allocator/canary.rs` — clean
* [x] `communication/src/allocator/thread.rs` — 2 findings (VecDeque never shrinks, dead recycling code)
* [x] `communication/src/allocator/zero_copy/mod.rs` — clean
* [x] `communication/src/allocator/zero_copy/bytes_slab.rs` — 2 findings (in_progress unbounded, stash limit timing)
* [x] `communication/src/allocator/zero_copy/bytes_exchange.rs` — 2 findings (MergeQueue unbounded, Drop allocates)
* [x] `communication/src/allocator/zero_copy/push_pull.rs` — clean
* [x] `communication/src/allocator/zero_copy/allocator.rs` — 3 findings (per-channel queue unbounded, staged never shrinks, missing capacity hint)
* [x] `communication/src/allocator/zero_copy/allocator_process.rs` — 3 findings (same patterns as allocator.rs)
* [x] `communication/src/allocator/zero_copy/tcp.rs` — 3 findings (unnecessary clone in unicast, stageds never shrink, stash never shrinks)
* [x] `communication/src/allocator/zero_copy/stream.rs` — clean
* [x] `communication/src/allocator/zero_copy/initialize.rs` — clean

### `timely/` — progress (8 files) — DONE
* [x] `timely/src/progress/mod.rs` — clean
* [x] `timely/src/progress/broadcast.rs` — 2 findings (logging allocates Vecs per send/recv, clone in recv)
* [x] `timely/src/progress/change_batch.rs` — clean (amortized compaction is by design)
* [x] `timely/src/progress/frontier.rs` — 2 findings (hash allocates temp Vec, rebuild clones)
* [x] `timely/src/progress/operate.rs` — clean
* [x] `timely/src/progress/reachability.rs` — 3 findings (worklist never shrinks, logging allocates Vecs, time clone per edge)
* [x] `timely/src/progress/subgraph.rs` — 3 findings (temp_active never shrinks, maybe_shutdown never shrinks, timestamp clones per target)
* [x] `timely/src/progress/timestamp.rs` — clean

### `timely/` — dataflow (all files) — DONE
* [x] `timely/src/dataflow/` — 15 findings (broadcast O(peers*records), capability double-update, notificator clone instead of move, EventLink per-event Rc, reclock O(n^2), exchange time clones, partition intermediate buffering, delay per-element Vec, handles staging never shrinks, counter double borrow_mut, etc.)

### `timely/` — top-level — DONE
* [x] `timely/src/lib.rs` — clean
* [x] `timely/src/worker.rs` — 3 findings (repeated logger lookup, dataflow format allocations, collect instead of take)
* [x] `timely/src/execute.rs` — clean
* [x] `timely/src/order.rs` — clean
* [x] `timely/src/logging.rs` — 1 finding (BatchLogger allocates Vec per progress event)
* [x] `timely/src/scheduling/activate.rs` — 4 findings (SyncActivator path clone, activate_after path alloc, BinaryHeap never shrinks, slices/bounds never shrink)
* [x] `timely/src/synchronization/barrier.rs` — 1 finding (Worker clone deep-clones Config)
* [x] `timely/src/synchronization/sequence.rs` — 2 findings (re-sort entire recvd, clone per peer)
