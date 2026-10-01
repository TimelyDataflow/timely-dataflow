# Panic audit log

## Findings by theme

### 1. Network I/O errors panic instead of returning errors

**Severity: high**

The entire distributed communication layer converts all I/O errors into panics.
Any network disruption, remote process crash, or ungraceful shutdown crashes the local process.
Two unchecked network-supplied array indices can cause out-of-bounds panics from corrupted or malicious messages.

**`communication/src/allocator/zero_copy/tcp.rs`** — The `tcp_panic` function (line 21) is the central panic point.
All read errors (line 73), write errors (line 192), flush errors (line 173), and shutdown errors (line 210) route through it.
EOF on the socket also panics (line 76).

* Line 100: `stageds[target - worker_offset]` — array index computed from network-supplied `MessageHeader` fields.
  If `target < worker_offset`, the subtraction wraps to `usize::MAX` (panic in debug, OOB in release).
  If `target - worker_offset >= stageds.len()`, it's out of bounds.
  A single corrupted message header crashes the receiver.

**`communication/src/networking.rs`** — Startup handshake panics on I/O errors.

* Line 127: `expect("failed to encode/send worker index")` — network failure after connect panics.
* Line 157: `expect("failed to decode worker index")` — remote crash or malformed connection panics.
* Line 158: `results[identifier - my_index - 1]` — `identifier` comes from the network with no validation.
  If `identifier <= my_index`, subtraction wraps. If `identifier` is too large, it's OOB.

**`timely/src/lib.rs`** — Deserialization of network messages.

* Line 147: `bincode::deserialize(&bytes[..]).expect(...)` — corrupt message data panics.
* Line 149: `assert_eq!(bytes.len(), (typed_size + 7) & !7)` — size mismatch panics.

**`timely/src/dataflow/channels/mod.rs`** — `Message::from_bytes` deserialization.

* Lines 71-74: `read_u64().unwrap()`, `bincode::deserialize_from().expect(...)` — malformed network bytes panic.

### 2. Drop implementations panic, causing abort during unwinding

**Severity: high**

Several `Drop` implementations call `expect()` on `JoinHandle::join()`.
If a worker or communication thread panicked (e.g., from network I/O errors above), the `Drop` impl panics too.
If the dropping thread is already unwinding from another panic, this causes a process abort.

* `communication/src/initialize.rs:417` — `WorkerGuards::drop()` calls `guard.join().expect("Worker panic")`.
  The public `join()` method (line 378) returns `Result`, but dropping without joining panics.

* `communication/src/allocator/zero_copy/initialize.rs:25,29` — `CommsGuard::drop()` calls `.expect("Send thread panic")` and `.expect("Recv thread panic")`.
  Network errors in tcp.rs cause send/recv threads to panic, then `CommsGuard::drop()` panics too.

* `communication/src/allocator/zero_copy/bytes_exchange.rs:119` — `MergeQueue::drop()` panics if the poison flag is set (by another thread's panic).
  If the dropping thread is already unwinding, this aborts.

### 3. Capture/replay I/O errors panic

**Severity: high**

The capture/replay mechanism has no error propagation path.

* `timely/src/dataflow/operators/core/capture/event.rs:163-165` — `EventWriter` calls `write_all().expect(...)` and `serialize_into().expect(...)`.
  A broken TCP connection or full disk during capture crashes the process.
  A TODO comment acknowledges: "push has no mechanism to report errors, so we unwrap."

* `timely/src/dataflow/operators/core/capture/event.rs:200` — `EventReader` calls `panic!("read failed: {e}")` on any I/O error other than `WouldBlock`.

* `timely/src/dataflow/operators/core/capture/event.rs:211` — `bincode::deserialize().expect(...)` on corrupt capture data panics.

### 4. User-facing APIs panic without safe alternatives

**Severity: high**

Some user-facing APIs panic on invalid input with no `try_*` or `Result`-returning alternative.

* `timely/src/dataflow/operators/core/input.rs:455` — `Handle::advance_to()` asserts `self.now_at.less_equal(&next)`.
  Calling with a non-monotonic time panics. No `try_advance_to` exists.

* `timely/src/dataflow/operators/vec/delay.rs:106,133` — `Delay::delay` and `delay_batch` assert that the user-supplied closure returns a time `>=` the input time.
  A buggy closure panics inside the dataflow runtime.

### 5. Capability API panics (safe alternatives exist)

**Severity: medium**

The capability API panics on misuse, but `try_*` alternatives exist for most methods.

* `timely/src/dataflow/operators/capability.rs:105` — `Capability::delayed` panics if `new_time` is not `>=` current time. Use `try_delayed` instead.
* `timely/src/dataflow/operators/capability.rs:140` — `Capability::downgrade` panics similarly. Use `try_downgrade`.
* `timely/src/dataflow/operators/capability.rs:449` — `CapabilitySet::delayed` panics. Use `try_delayed`.
* `timely/src/dataflow/operators/capability.rs:486` — `CapabilitySet::downgrade` panics. Use `try_downgrade`.
* `timely/src/dataflow/operators/capability.rs:287` — `InputCapability::delayed` panics on invalid time or disconnected output. No `try_*` alternative exists for this one.

### 6. `try_regenerate` panics despite `try_` naming convention

**Severity: medium**

* `bytes/src/lib.rs:132` — `BytesMut::try_regenerate::<B>()` calls `downcast_mut::<B>().expect("Downcast failed")`.
  If the type parameter `B` doesn't match the original allocation type, this panics instead of returning `false`.
  The `try_` prefix strongly implies graceful failure.
  The only internal call site (`bytes_slab.rs:83`) uses the correct type, but the method is public API.

### 7. Sequencer panics if used before first worker step

**Severity: medium**

* `timely/src/synchronization/sequence.rs:212` — `Sequencer::push()` calls `unwrap()` on the activator, which is lazily initialized on the first `worker.step()`. Calling `push()` before stepping panics.

* `timely/src/synchronization/sequence.rs:229` — `Sequencer::drop()` calls `expect("Sequencer.activator unavailable")`. Dropping a `Sequencer` before the first step panics.

### 8. Initialization failures cascade via channel send/recv expects

**Severity: medium**

During multi-worker initialization, if any thread dies before setup completes, the surviving threads panic when their channel operations fail.

* `communication/src/allocator/process.rs:39,43` — `expect("Failed to send/recv buzzer")`
* `communication/src/allocator/zero_copy/allocator.rs:91,98` — `expect("Failed to send/receive MergeQueue")`
* `communication/src/allocator/zero_copy/allocator_process.rs:70,77` — same pattern
* `communication/src/allocator/zero_copy/tcp.rs:48,151` — same pattern

### 9. Mutex poisoning propagation

**Severity: medium**

* `communication/src/allocator/process.rs:123` — `self.channels.lock().expect("mutex error?")`.
  If any thread panics while holding this lock, all subsequent `allocate()` calls on other threads panic.

* `communication/src/allocator/zero_copy/bytes_exchange.rs:47,55,62,96,103` — `MergeQueue` checks a poison flag and panics if set. Additionally, `lock().expect(...)` on the internal mutex propagates poisoning.

### 10. Operator implementation validation panics

**Severity: medium**

Users implementing the `Operate` trait or using low-level builder APIs can trigger panics from invalid configurations.

* `timely/src/progress/subgraph.rs:657-666` — `assert_eq!` and `assert!` on connectivity summary dimensions.
  A custom `Operate` impl returning wrong-sized connectivity panics.

* `timely/src/progress/subgraph.rs:161` — `assert!` that children have contiguous indices.

* `timely/src/progress/operate.rs:93,96` — `PortConnectivity::add_port` panics on duplicate port entries.

* `timely/src/dataflow/operators/generic/builder_raw.rs:117` — `assert!` that connectivity references valid output ports.

### 11. Reachability tracker unchecked indexing

**Severity: low-medium**

The `Tracker` and `Builder` in `timely/src/progress/reachability.rs` use unchecked array indexing on node/port indices throughout.
These are internal APIs not typically called by users directly, but a custom `Operate` implementation could supply out-of-bounds indices.

* Lines 188, 599, 621, 661, 684, 717, 728 — `self.per_operator[node]`, `self.edges[node][port]`, etc.

### 12. Minor findings

* `container/src/lib.rs:195` — `i64::try_from(Vec::len(self)).unwrap()` in `record_count()`. Only panics for `Vec<()>` with >2^63 elements (practically unreachable).

* `bytes/src/lib.rs:92,202` — `assert!(index <= self.len)` in `extract_to`. Standard bounds check, but the error message is unhelpful.

* `timely/src/worker.rs:245,252,259` — `panic!("Unacceptable address: Length zero")` in `allocate`/`pipeline`/`broadcast`. Addresses are framework-generated, so this is effectively an internal invariant.

* `logging/src/lib.rs:168,263,303` — `RefCell::borrow_mut()` panics on re-entrant logging (calling `log()` from within a log action callback).

* `timely/src/progress/subgraph.rs:720` — `panic!()` when a shut-down operator receives frontier changes. Possible timing issue, but the framework normally prevents this.
