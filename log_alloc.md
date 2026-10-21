# Allocation audit log

## Findings by theme

### 1. Broadcast clones every record `peers` times

**Severity: medium-high**

* `timely/src/dataflow/operators/vec/broadcast.rs:31` — The broadcast operator is implemented as `flat_map(|x| (0..peers).map(|i| (i, x.clone()))).exchange(|ix| ix.0).map(|(_i,x)| x)`.
  Each record is cloned `peers` times, wrapped in tuples, exchanged, then unwrapped.
  This is O(peers * records) in allocations.
  The comment acknowledges: "Simplified implementation... Optimize once they have settled down."

### 2. Per-message event notifications without batching

**Severity: medium**

The inter-thread communication path does three operations per message push with no batching:
* `communication/src/allocator/counters.rs:47-49` — `events.push(self.index)` appends to a shared Vec on every push, growing O(messages) between drains.
* `communication/src/allocator/counters.rs:102` — `self.buzzer.buzz()` calls unpark/condvar on every push, even when the target thread is already awake.
* `communication/src/allocator/process.rs:189-194` — `receive()` drains all pending mpsc messages into the events Vec in one shot, no bound or backpressure.

Commented-out code in `counters.rs:34-44` shows a batching strategy was considered but not completed.

### 3. Unbounded buffer growth throughout the communication layer

**Severity: medium**

Multiple buffers grow to their high-water mark and never shrink:

* `communication/src/allocator/zero_copy/bytes_slab.rs:106` — `in_progress` Vec grows as buffers are retired, never shrinks. Slow consumers cause monotonic growth.
* `communication/src/allocator/zero_copy/bytes_exchange.rs:31` — `MergeQueue` VecDeque grows without backpressure under producer-consumer imbalance.
* `communication/src/allocator/zero_copy/allocator.rs:277-289` and `allocator_process.rs:204-216` — Per-channel `VecDeque<Bytes>` grows without limit if consumers are slow.
* `communication/src/allocator/zero_copy/tcp.rs:53-56` — `stageds` inner Vecs retain peak capacity.
* `communication/src/allocator/zero_copy/allocator.rs:128` and `allocator_process.rs:118` — `staged` Vec retains high-water-mark capacity.

### 4. Capability operations are heavier than necessary

**Severity: medium**

* `timely/src/dataflow/operators/capability.rs:154-161` — `try_downgrade` creates a new intermediate `Capability` (incrementing ChangeBatch), then drops the old one (decrementing). Two `borrow_mut` + `update` calls when one in-place update would suffice.
* `timely/src/dataflow/operators/generic/notificator.rs:323` — `make_available` clones capabilities from `pending` instead of moving them. A TODO comment acknowledges this.
* `timely/src/dataflow/operators/capability.rs:167-171` — `Capability::drop` clones the time to call `update(time.clone(), -1)` because `update` takes ownership.

### 5. Repeated string-based logger lookup on every step

**Severity: medium**

* `timely/src/worker.rs:391,401` — `self.logging()` is called multiple times per `step_or_park()`. Each call goes through `self.log_register()` → `borrow()` → `HashMap::get("timely")`, performing a string lookup on every worker step.
  Should be cached in the `Worker` struct.

### 6. EventLink allocates one Rc per captured event

**Severity: medium**

* `timely/src/dataflow/operators/core/capture/event.rs:75` — Every pushed event creates a new `Rc<EventLink>`. For high-throughput capture, this is one heap allocation per event.
  A pre-allocated ring buffer or arena would be more efficient.

### 7. Reclock operator has O(n^2) stash behavior

**Severity: medium**

* `timely/src/dataflow/operators/core/reclock.rs:55-79` — The stash is a `Vec` scanned linearly per notification, then `retain` shifts elements. With many distinct timestamps this becomes O(n^2).
  A `BTreeMap<T, Vec<C>>` would give O(log n) lookups and efficient range removal.

### 8. Logging allocates Vecs in the hot path

**Severity: medium (when logging enabled)**

* `timely/src/progress/broadcast.rs:66-67,120-121` — Every `send()`/`recv()` allocates two `Vec`s for logging that are transferred to the logger by ownership.
* `timely/src/progress/reachability.rs:852,867` — `log_source_updates`/`log_target_updates` collect into new Vecs, cloning every timestamp.
* `timely/src/logging.rs:51` — `BatchLogger::publish_batch` allocates a 2-element Vec per progress frontier advance.

### 9. `BytesRefill` double indirection

**Severity: medium**

* `communication/src/initialize.rs:157` — Default refill closure creates `Box::new(vec![0_u8; size])`: the Vec already heap-allocates its buffer, then Box adds another heap allocation and pointer indirection.
  `vec![0u8; size].into_boxed_slice()` would eliminate the Vec metadata overhead.

### 10. Unnecessary clone in TCP receive unicast path

**Severity: low-medium**

* `communication/src/allocator/zero_copy/tcp.rs:99-101` — `bytes.clone()` for every target in the range. For unicast messages (the common case where `target_upper - target_lower == 1`), the original `bytes` could be moved instead of cloned, saving one atomic refcount increment/decrement pair per message.

### 11. `SyncActivator` and delayed activations allocate path Vecs

**Severity: low-medium**

* `timely/src/scheduling/activate.rs:280` — `SyncActivator::activate()` clones `self.path` (a `Vec<usize>`) on every call.
* `timely/src/scheduling/activate.rs:87` — `activate_after` allocates `path.to_vec()` per delayed activation.
  Using `Rc<[usize]>` would avoid per-call allocation.

### 12. Exchange partition clones time per container extraction

**Severity: low-medium**

* `timely/src/dataflow/channels/pushers/exchange.rs:57,67` — `time.clone()` inside the per-container extraction loop. For complex product timestamps, this adds up.

### 13. Sequencer inefficiencies

**Severity: low-medium**

* `timely/src/synchronization/sequence.rs:185` — Sink re-sorts the entire `recvd` vector each invocation, including already-sorted elements. Should sort only new elements and merge.
* `timely/src/synchronization/sequence.rs:153` — Clones each element `peers - 1` times; the last iteration could move.

### 14. Thread allocator dead code

**Severity: low**

* `communication/src/allocator/thread.rs:61` — The shared tuple contains two VecDeques but the recycling code using the second one (lines 97-102) is commented out. The second VecDeque is allocated but never used.

### 15. Partition operator intermediate buffering

**Severity: low-medium**

* `timely/src/dataflow/operators/core/partition.rs:61-67` — Creates a `BTreeMap<u64, Vec<_>>` to buffer data per partition before pushing to outputs. Data could be pushed directly to per-output container builders without the intermediate collection.

### 16. Minor findings

* `container/src/lib.rs:150` — `CapacityContainerBuilder::pending` VecDeque grows but never shrinks. `relax()` is a no-op.
* `container/src/lib.rs:216-219` — `ensure_capacity` computes `reserve(preferred - capacity())` but should use `reserve(preferred - len())`.
* Various `BinaryHeap` and `Vec` instances across the codebase that drain but never shrink (standard amortized pattern, acceptable in most cases).
* `timely/src/synchronization/barrier.rs:23` — `Worker::clone()` deep-clones `Config` (which contains a `HashMap`), but Config could be `Arc`-wrapped.
