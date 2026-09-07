# Hot loop optimization audit tracker

Look for:
* Loop-invariant branches that could be hoisted
* Virtual dispatch (dyn trait calls) inside tight loops
* Iterator chains that prevent auto-vectorization
* Data layouts that prevent SIMD (AoS vs SoA, non-contiguous memory)
* Indirect indexing patterns that block vectorization
* Branchy per-element logic that could be branchless or batched

## Crates and areas

### `bytes/` (1 file) — DONE
* [x] `bytes/src/lib.rs` — 1 finding (Arc<dyn Any> overhead, low impact)

### `container/` (1 file) — DONE
* [x] `container/src/lib.rs` — 2 findings (per-element capacity check prevents vectorization, ensure_capacity per push)

### `logging/` (1 file) — DONE
* [x] `logging/src/lib.rs` — 3 findings (dyn FnMut action dispatch, RefCell per log, time.elapsed per log)

### `communication/` (20 files) — DONE
* [x] `communication/src/allocator/` — 3 findings (per-element event push+buzzer, RefCell per push in counters)
* [x] `communication/src/allocator/zero_copy/` — 6 findings (dyn Pull vtable, Rc<RefCell<SendEndpoint>> per push, HashMap per message in receive, spinlock without PAUSE, MergeQueue lock hold, logger loop-invariant)
* [x] `communication/src/` (top-level) — covered by zero_copy findings

### `timely/` — progress (8 files) — DONE
* [x] `timely/src/progress/change_batch.rs` — 2 findings (re-sorts clean prefix, two-pass consolidation)
* [x] `timely/src/progress/frontier.rs` — 2 findings (redundant frontier scans, O(n*m) rebuild)
* [x] `timely/src/progress/reachability.rs` — 1 finding (BinaryHeap worklist overhead)
* [x] `timely/src/progress/subgraph.rs` — 2 findings (virtual dispatch through dyn Schedule, RefCell borrow churn)
* [x] `timely/src/progress/operate.rs` — 1 finding (BTreeMap for 1-2 ports)
* [x] `timely/src/progress/broadcast.rs` — 1 finding (per-element clone in recv)
* [x] `timely/src/progress/` (remaining) — clean

### `timely/` — dataflow (all files) — DONE
* [x] `timely/src/dataflow/channels/` — 5 findings (Counter+Tee per-message overhead, exchange random cache access, exchange inline(never), dyn PushSet dispatch)
* [x] `timely/src/dataflow/operators/` — 7 findings (delay per-datum assert+HashMap, filter per-element vs retain, partition BTreeMap, branch per-datum give, map via flat_map, handles staging sort, for_each_time grouping)

### `timely/` — top-level — DONE
* [x] `timely/src/worker.rs` — 4 findings (HashMap per channel event, borrow_mut inside loop, RefCell churn on activations, logger checks)
* [x] `timely/src/scheduling/activate.rs` — 3 findings (unconditional compaction, indirect slice sort, BinaryHeap)
* [x] `timely/src/` (remaining) — clean
