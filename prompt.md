# Bug audit tracker

## Crates and areas

### `bytes/` (1 file) — DONE
* [x] `bytes/src/lib.rs`

### `container/` (1 file) — DONE
* [x] `container/src/lib.rs`

### `logging/` (1 file) — DONE
* [x] `logging/src/lib.rs`

### `communication/` (20 files) — DONE
* [x] `communication/src/lib.rs`
* [x] `communication/src/initialize.rs`
* [x] `communication/src/buzzer.rs`
* [x] `communication/src/logging.rs`
* [x] `communication/src/networking.rs`
* [x] `communication/src/allocator/mod.rs`
* [x] `communication/src/allocator/process.rs`
* [x] `communication/src/allocator/counters.rs`
* [x] `communication/src/allocator/generic.rs`
* [x] `communication/src/allocator/canary.rs`
* [x] `communication/src/allocator/thread.rs`
* [x] `communication/src/allocator/zero_copy/mod.rs`
* [x] `communication/src/allocator/zero_copy/bytes_slab.rs`
* [x] `communication/src/allocator/zero_copy/bytes_exchange.rs`
* [x] `communication/src/allocator/zero_copy/push_pull.rs`
* [x] `communication/src/allocator/zero_copy/allocator.rs` — 1 bug found
* [x] `communication/src/allocator/zero_copy/allocator_process.rs`
* [x] `communication/src/allocator/zero_copy/tcp.rs`
* [x] `communication/src/allocator/zero_copy/stream.rs`
* [x] `communication/src/allocator/zero_copy/initialize.rs`

### `timely/` — progress (8 files) — DONE
* [x] `timely/src/progress/mod.rs`
* [x] `timely/src/progress/broadcast.rs` — 1 minor bug found
* [x] `timely/src/progress/change_batch.rs` — 1 bug found
* [x] `timely/src/progress/frontier.rs`
* [x] `timely/src/progress/operate.rs`
* [x] `timely/src/progress/reachability.rs`
* [x] `timely/src/progress/subgraph.rs`
* [x] `timely/src/progress/timestamp.rs`

### `timely/` — dataflow/channels (8 files) — DONE
* [x] `timely/src/dataflow/channels/mod.rs`
* [x] `timely/src/dataflow/channels/pact.rs`
* [x] `timely/src/dataflow/channels/pullers/`
* [x] `timely/src/dataflow/channels/pushers/`

### `timely/` — dataflow/operators/generic (7 files) — DONE
* [x] `timely/src/dataflow/operators/generic/builder_raw.rs`
* [x] `timely/src/dataflow/operators/generic/builder_rc.rs`
* [x] `timely/src/dataflow/operators/generic/builder_ref.rs` (empty/stub)
* [x] `timely/src/dataflow/operators/generic/handles.rs`
* [x] `timely/src/dataflow/operators/generic/notificator.rs` — 1 bug found
* [x] `timely/src/dataflow/operators/generic/operator_info.rs` (trivial)
* [x] `timely/src/dataflow/operators/generic/operator.rs`

### `timely/` — dataflow/operators/capability — DONE
* [x] `timely/src/dataflow/operators/capability.rs`

### `timely/` — dataflow/operators/core (20 files) — DONE
* [x] `timely/src/dataflow/operators/core/mod.rs`
* [x] `timely/src/dataflow/operators/core/input.rs`
* [x] `timely/src/dataflow/operators/core/unordered_input.rs`
* [x] `timely/src/dataflow/operators/core/exchange.rs`
* [x] `timely/src/dataflow/operators/core/feedback.rs`
* [x] `timely/src/dataflow/operators/core/enterleave.rs`
* [x] `timely/src/dataflow/operators/core/concat.rs`
* [x] `timely/src/dataflow/operators/core/filter.rs`
* [x] `timely/src/dataflow/operators/core/inspect.rs`
* [x] `timely/src/dataflow/operators/core/map.rs`
* [x] `timely/src/dataflow/operators/core/ok_err.rs`
* [x] `timely/src/dataflow/operators/core/partition.rs`
* [x] `timely/src/dataflow/operators/core/probe.rs`
* [x] `timely/src/dataflow/operators/core/rc.rs`
* [x] `timely/src/dataflow/operators/core/reclock.rs`
* [x] `timely/src/dataflow/operators/core/to_stream.rs`
* [x] `timely/src/dataflow/operators/core/capture/mod.rs`
* [x] `timely/src/dataflow/operators/core/capture/event.rs`
* [x] `timely/src/dataflow/operators/core/capture/capture.rs`
* [x] `timely/src/dataflow/operators/core/capture/replay.rs`
* [x] `timely/src/dataflow/operators/core/capture/extract.rs`

### `timely/` — dataflow/operators/vec (17 files) — DONE
* [x] `timely/src/dataflow/operators/vec/mod.rs`
* [x] `timely/src/dataflow/operators/vec/input.rs`
* [x] `timely/src/dataflow/operators/vec/unordered_input.rs`
* [x] `timely/src/dataflow/operators/vec/broadcast.rs`
* [x] `timely/src/dataflow/operators/vec/count.rs`
* [x] `timely/src/dataflow/operators/vec/branch.rs`
* [x] `timely/src/dataflow/operators/vec/delay.rs`
* [x] `timely/src/dataflow/operators/vec/filter.rs`
* [x] `timely/src/dataflow/operators/vec/flow_controlled.rs`
* [x] `timely/src/dataflow/operators/vec/map.rs`
* [x] `timely/src/dataflow/operators/vec/partition.rs`
* [x] `timely/src/dataflow/operators/vec/queue.rs` (dead code, not in mod.rs)
* [x] `timely/src/dataflow/operators/vec/result.rs`
* [x] `timely/src/dataflow/operators/vec/to_stream.rs`
* [x] `timely/src/dataflow/operators/vec/aggregation/mod.rs`
* [x] `timely/src/dataflow/operators/vec/aggregation/aggregate.rs`
* [x] `timely/src/dataflow/operators/vec/aggregation/state_machine.rs`

### `timely/` — dataflow/scopes (2 files) — DONE
* [x] `timely/src/dataflow/scopes/mod.rs`
* [x] `timely/src/dataflow/scopes/child.rs`

### `timely/` — top-level (11 files) — DONE
* [x] `timely/src/lib.rs`
* [x] `timely/src/worker.rs` — 1 bug found
* [x] `timely/src/execute.rs`
* [x] `timely/src/order.rs`
* [x] `timely/src/logging.rs`
* [x] `timely/src/dataflow/stream.rs`
* [x] `timely/src/scheduling/activate.rs`
* [x] `timely/src/synchronization/mod.rs`
* [x] `timely/src/synchronization/barrier.rs`
* [x] `timely/src/synchronization/sequence.rs`
