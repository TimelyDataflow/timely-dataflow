# Silent error swallowing audit tracker

## Crates and areas

### `bytes/` (1 file) — DONE
* [x] `bytes/src/lib.rs` — clean

### `container/` (1 file) — DONE
* [x] `container/src/lib.rs` — clean (unwrap_or_default is intentional)

### `logging/` (1 file) — DONE
* [x] `logging/src/lib.rs` — clean

### `communication/` (20 files) — DONE
* [x] `communication/src/lib.rs` — clean
* [x] `communication/src/initialize.rs` — clean
* [x] `communication/src/buzzer.rs` — clean
* [x] `communication/src/logging.rs` — clean
* [x] `communication/src/networking.rs` — 1 finding (.ok() on cursor read, intentional)
* [x] `communication/src/allocator/mod.rs` — clean
* [x] `communication/src/allocator/process.rs` — 2 findings (silent send, try_recv conflation)
* [x] `communication/src/allocator/counters.rs` — 1 finding (dropped event notification)
* [x] `communication/src/allocator/generic.rs` — clean
* [x] `communication/src/allocator/canary.rs` — clean
* [x] `communication/src/allocator/thread.rs` — clean
* [x] `communication/src/allocator/zero_copy/mod.rs` — clean
* [x] `communication/src/allocator/zero_copy/bytes_slab.rs` — clean
* [x] `communication/src/allocator/zero_copy/bytes_exchange.rs` — clean
* [x] `communication/src/allocator/zero_copy/push_pull.rs` — clean
* [x] `communication/src/allocator/zero_copy/allocator.rs` — 3 findings (infinite spin-loop, silent data drop, buffered data drop)
* [x] `communication/src/allocator/zero_copy/allocator_process.rs` — 3 findings (same patterns)
* [x] `communication/src/allocator/zero_copy/tcp.rs` — clean (all errors panic via tcp_panic)
* [x] `communication/src/allocator/zero_copy/stream.rs` — clean
* [x] `communication/src/allocator/zero_copy/initialize.rs` — clean

### `timely/` — progress (8 files) — DONE
* [x] `timely/src/progress/mod.rs` — clean
* [x] `timely/src/progress/broadcast.rs` — clean (conditional logging only)
* [x] `timely/src/progress/change_batch.rs` — clean
* [x] `timely/src/progress/frontier.rs` — clean (discarded update_iter in constructors)
* [x] `timely/src/progress/operate.rs` — clean
* [x] `timely/src/progress/reachability.rs` — 1 finding (cycle detection prints to stdout)
* [x] `timely/src/progress/subgraph.rs` — clean
* [x] `timely/src/progress/timestamp.rs` — clean

### `timely/` — dataflow (all files) — DONE
* [x] `timely/src/dataflow/` (channels, operators, scopes, stream) — 1 finding (capture send silently dropped)

### `timely/` — top-level — DONE
* [x] `timely/src/lib.rs` — clean
* [x] `timely/src/worker.rs` — 2 findings (unknown channel ignored with TODO, optional logging)
* [x] `timely/src/execute.rs` — clean
* [x] `timely/src/order.rs` — clean
* [x] `timely/src/logging.rs` — clean
* [x] `timely/src/scheduling/activate.rs` — 1 finding (delay ignored when timer is None)
* [x] `timely/src/synchronization/barrier.rs` — clean
* [x] `timely/src/synchronization/sequence.rs` — clean (Weak cleanup is intentional)
