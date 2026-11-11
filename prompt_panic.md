# Panic audit tracker

## Crates and areas

### `bytes/` (1 file) — DONE
* [x] `bytes/src/lib.rs` — 3 findings (try_regenerate naming, extract_to asserts)

### `container/` (1 file) — DONE
* [x] `container/src/lib.rs` — 1 finding (record_count overflow for ZSTs)

### `logging/` (1 file) — DONE
* [x] `logging/src/lib.rs` — 1 finding (RefCell re-entrancy)

### `communication/` (20 files) — DONE
* [x] `communication/src/lib.rs` — clean
* [x] `communication/src/initialize.rs` — 2 findings (WorkerGuards::drop, assert_eq)
* [x] `communication/src/buzzer.rs` — clean
* [x] `communication/src/logging.rs` — clean
* [x] `communication/src/networking.rs` — 5 findings (I/O expects, unchecked network index)
* [x] `communication/src/allocator/mod.rs` — clean
* [x] `communication/src/allocator/process.rs` — 5 findings (mutex poison, channel expects, downcast)
* [x] `communication/src/allocator/counters.rs` — clean
* [x] `communication/src/allocator/generic.rs` — clean
* [x] `communication/src/allocator/canary.rs` — clean
* [x] `communication/src/allocator/thread.rs` — clean
* [x] `communication/src/allocator/zero_copy/mod.rs` — clean
* [x] `communication/src/allocator/zero_copy/bytes_slab.rs` — clean
* [x] `communication/src/allocator/zero_copy/bytes_exchange.rs` — 7 findings (MergeQueue poison, mutex, Drop panic)
* [x] `communication/src/allocator/zero_copy/push_pull.rs` — 3 findings (asserts, expect on header write)
* [x] `communication/src/allocator/zero_copy/allocator.rs` — 5 findings (channel expects, identifier asserts)
* [x] `communication/src/allocator/zero_copy/allocator_process.rs` — 4 findings (channel expects, identifier assert)
* [x] `communication/src/allocator/zero_copy/tcp.rs` — 11 findings (tcp_panic on all I/O, unchecked array index, MergeQueue expect)
* [x] `communication/src/allocator/zero_copy/stream.rs` — clean
* [x] `communication/src/allocator/zero_copy/initialize.rs` — 4 findings (CommsGuard::drop expects, set_nonblocking expect)

### `timely/` — progress (8 files) — DONE
* [x] `timely/src/progress/mod.rs` — clean
* [x] `timely/src/progress/broadcast.rs` — clean
* [x] `timely/src/progress/change_batch.rs` — clean (indexing is guarded)
* [x] `timely/src/progress/frontier.rs` — clean (debug_assert only)
* [x] `timely/src/progress/operate.rs` — 1 finding (add_port duplicate panic)
* [x] `timely/src/progress/reachability.rs` — 6 findings (unchecked indexing throughout, unwraps in is_acyclic)
* [x] `timely/src/progress/subgraph.rs` — 5 findings (children index assert, child 0 shape assert, operator validation, premature shutdown panic, validate_progress panic)
* [x] `timely/src/progress/timestamp.rs` — clean

### `timely/` — dataflow (all files) — DONE
* [x] `timely/src/dataflow/` (channels, operators, scopes, stream) — 16 findings (deserialization panics, capability panics, advance_to assert, delay asserts, capture I/O panics, tee add_pusher)

### `timely/` — top-level — DONE
* [x] `timely/src/lib.rs` — 4 findings (deserialize expect, size assert, serialize expects)
* [x] `timely/src/worker.rs` — 3 findings (empty address panics)
* [x] `timely/src/execute.rs` — clean
* [x] `timely/src/order.rs` — clean
* [x] `timely/src/logging.rs` — clean
* [x] `timely/src/scheduling/activate.rs` — clean (all guarded)
* [x] `timely/src/synchronization/barrier.rs` — clean (overflow infeasible)
* [x] `timely/src/synchronization/sequence.rs` — 3 findings (activator unwrap before step, Drop expect before step)
