# Hot loop optimization audit log

## Findings by theme

### 1. Per-message overhead on every dataflow edge (Counter + Tee chain)

**Severity: high**

Every edge in the dataflow chains: `Counter::push` → `Tee::push` → `{PushOne|PushMany}::push` → downstream.
Per message this costs: 2 `RefCell::borrow_mut()` checks, 1-2 vtable calls, 1 timestamp clone, 1 `ChangeBatch::update`.

* `timely/src/dataflow/channels/pushers/tee.rs:86` — `RefCell::borrow_mut()` on every message through every Tee.
* `timely/src/dataflow/channels/pushers/tee.rs:87` — Virtual dispatch through `Box<dyn PushSet>` on every message. Could be an enum (`PushOne | PushMany`) to allow inlining.
* `timely/src/dataflow/channels/pushers/counter.rs:21-26` — Two separate `borrow_mut()` calls per push (one for `update`, one for `is_empty` check). Could be a single borrow scope.
* `timely/src/dataflow/channels/pushers/counter.rs:22` — `message.time.clone()` per message. A "last seen time" cache could batch same-time updates.

Fusing Counter and Tee into a single struct would eliminate one `RefCell` round-trip. Replacing `Box<dyn PushSet>` with an enum would allow inlining the common single-consumer case.

### 2. Per-element event push and buzzer syscall in inter-thread communication

**Severity: high**

* `communication/src/allocator/counters.rs:47-49` — `Pusher::push` does `events.borrow_mut().push(self.index)` per element. Commented-out code shows a batching strategy that would reduce to O(flushes).
* `communication/src/allocator/counters.rs:99,102` — `ArcPusher::push` does `events.send(self.index)` (mpsc send, involves mutex) and `buzzer.buzz()` (thread unpark syscall) per element. Batching to flush boundaries would save ~999 mutex acquisitions and syscalls per 1000-element batch.

### 3. Exchange pusher: random cache access in hash-routing loop

**Severity: high**

* `timely/src/dataflow/channels/pushers/exchange.rs:54-55` — `builders[index]` with hash-derived index causes random cache-line access across the builders vector. With many peers, this thrashes L1 cache.
* The existing `TODO` comment on line 88 already notes the need for software write-combining (SWC): buffer small staging areas per target, flush when full.
* Line 53-58: `extract()` is called per datum inside the inner loop, checking builder fullness every element. Separating the hash-routing loop from container extraction would enable tighter codegen.
* Line 112: `#[inline(never)]` on `Exchange::push` prevents inlining the single-pusher fast path.

### 4. Virtual dispatch through `Box<dyn Pull<T>>` on every message receive

**Severity: medium-high**

* `communication/src/allocator/zero_copy/push_pull.rs:122-140` — `PullerInner::pull` calls `self.inner.pull()` through `Box<dyn Pull<T>>` on every message. This is the primary data receive path for `TcpAllocator`. Making `PullerInner` generic over the inner puller type would monomorphize the pull path and allow inlining.

### 5. `ChangeBatch::compact()` re-sorts clean prefix and uses two-pass consolidation

**Severity: medium-high**

* `timely/src/progress/change_batch.rs:293` — `compact()` sorts the entire vector including the already-sorted clean prefix. For workloads where a few dirty elements are appended to a large clean batch, a merge-based approach (sort only dirty suffix, then merge) would reduce from O(n log n) to O(n).
* `timely/src/progress/change_batch.rs:294-304` — Three passes (sort, forward-scan-and-zero, retain). A single dedup-accumulate pass would eliminate the `retain` pass and improve cache utilization.

### 6. Delay operator: per-datum assert + HashMap lookup + timestamp clone

**Severity: high**

* `timely/src/dataflow/operators/vec/delay.rs:106` — `assert!(time.time().less_equal(&new_time))` per datum in release mode. Should be `debug_assert!`.
* `timely/src/dataflow/operators/vec/delay.rs:107-109` — Per-datum HashMap lookup with `new_time.clone()` for the entry API. A last-seen-time cache would avoid repeated lookups when most data maps to the same time.
* `delay_total` delegates to `delay` without specializing for total-order timestamps.

### 7. `PortConnectivity` uses `BTreeMap` for typically 1-2 ports

**Severity: medium**

* `timely/src/progress/operate.rs:70-107` — `BTreeMap<usize, Antichain<TS>>` used in `PortConnectivity`, iterated via `iter_ports()` on every frontier change in the propagation loop. A `SmallVec<[(usize, Antichain<TS>); 2]>` would give contiguous memory access for the overwhelmingly common 1-2 port case.

### 8. `BinaryHeap` worklist in `Tracker::propagate_all()`

**Severity: medium**

* `timely/src/progress/reachability.rs:645-702` — The worklist uses `BinaryHeap` with O(log n) per push/pop. Many entries are duplicates that cancel immediately. A sort-based approach (accumulate in Vec, sort once, process in order) would have better cache locality and constants.

### 9. HashMap lookups in the worker step loop

**Severity: medium**

* `timely/src/worker.rs:367` — `paths.get(&channel)` HashMap lookup per channel event. Channel IDs are dense integers allocated sequentially — a `Vec` indexed by channel ID would give O(1) access.
* `timely/src/worker.rs:413` — `dataflows.entry(index)` HashMap lookup per active dataflow. Same — dataflow indices are sequential, suitable for `Vec<Option<Wrapper>>`.
* `timely/src/worker.rs:368-370` — `self.activations.borrow_mut()` inside the channel event loop. Should be hoisted outside.

### 10. `RefCell::borrow_mut()` churn on activations during step

**Severity: medium**

* `timely/src/worker.rs:368-408` — Four separate `RefCell` borrow/unborrow cycles on `self.activations` per step. A single borrow scope for the channel-event + advance + for_extensions phase would reduce overhead.

### 11. `Activations::advance()` unconditional compaction

**Severity: medium**

* `timely/src/scheduling/activate.rs:122-128` — Every `advance()` copies all active path slices from `self.slices` into `self.buffer`, even when no new activations were added. A dirty flag would make idle steps essentially free.
* `timely/src/scheduling/activate.rs:117-119` — Activation path sorting uses indirect slice comparisons. Packing short paths into fixed-size keys would enable direct value comparisons.

### 12. Partition operator uses BTreeMap for dense integer indices

**Severity: medium**

* `timely/src/dataflow/operators/core/partition.rs:62-67` — Per-datum `BTreeMap` lookup where partition indices are 0..parts. A `Vec<Vec<_>>` indexed by partition number converts O(log k) into O(1).

### 13. Filter/Branch/OkErr per-element give instead of batch operations

**Severity: medium**

* `timely/src/dataflow/operators/core/filter.rs:33-34` — Core filter uses per-element `give_iterator` with `filter`. The vec variant already uses `retain` for in-place filtering.
* `timely/src/dataflow/operators/vec/branch.rs:58-66` — Per-datum `give()` to two output sessions. Partitioning into two local Vecs first then `give_container` would batch the output.

### 14. `Rc<RefCell<SendEndpoint>>` indirection per serialized push

**Severity: medium**

* `communication/src/allocator/zero_copy/push_pull.rs:36-58` — Every outgoing message does `Rc` pointer chase + `RefCell::borrow_mut()` + borrow flag manipulation. Multiple Pushers share a single SendEndpoint; batching the borrow per push batch would reduce overhead.

### 15. MergeQueue lock held during entire VecDeque drain

**Severity: medium**

* `communication/src/allocator/zero_copy/bytes_exchange.rs:94-106` — `drain_into` holds the mutex for the entire drain. Swapping the queue with an empty VecDeque under the lock and draining outside would minimize lock hold time.

### 16. Spinlock without PAUSE hint

**Severity: low-medium**

* `communication/src/allocator/zero_copy/bytes_exchange.rs:57-62,98-103` — `MergeQueue` spin-lock loops without `std::hint::spin_loop()`. Missing PAUSE instruction causes excessive cache-line bouncing under contention.

### 17. `MutableAntichain::update_iter()` redundant frontier scans

**Severity: low-medium**

* `timely/src/progress/frontier.rs:539-550` — Two separate linear scans of `self.frontier` per update (one for `less_than`, one for `less_equal`). Could be fused into a single scan.

### 18. `Progcaster::recv()` per-element clone and ChangeBatch updates

**Severity: medium**

* `timely/src/progress/broadcast.rs:147-149` — Each received progress update is individually cloned and pushed via `update()`, potentially triggering `maintain_bounds`/`compact()` mid-receive. Batching with `extend` would defer compaction.

### 19. `CapacityContainerBuilder` per-element capacity check prevents bulk vectorization

**Severity: medium**

* `container/src/lib.rs:153-167` — `push_into` checks `ensure_capacity` and `at_capacity` per element. A bulk `extend`-style method that copies elements in chunks up to remaining capacity would enable auto-vectorization for `Copy` types.

### 20. Loop-invariant logger checks in TCP send/recv loops

**Severity: low**

* `communication/src/allocator/zero_copy/tcp.rs:87-96` — `logger.as_mut().map(...)` per iteration. Logger presence is loop-invariant.
* `communication/src/allocator/zero_copy/tcp.rs:181-192` — When logging enabled, send loop redundantly re-parses message headers just for logging.
* `timely/src/worker.rs:776-789` — Two `Option` checks on `self.logging` per `Wrapper::step()`. Branch predictor handles this well.

### 21. `for_each_time` sorts staging on every operator invocation

**Severity: low-medium**

* `timely/src/dataflow/operators/generic/handles.rs:56-71` — Sorts entire staging deque by time on every call. Messages from a single source (pipeline pact) are already time-ordered; could skip the sort in that case.
