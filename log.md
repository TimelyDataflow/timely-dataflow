# Bug audit log

## `timely/src/progress/change_batch.rs`

### `compact()` does not remove single zero-valued entries

`compact()` at line 292 guards the compaction body with `self.updates.len() > 1`.
A batch with a single `(key, 0)` entry will never have that entry removed.
Subsequent `is_empty()` returns `false` for a logically empty batch because the `clean > len/2` fast path fires.

Reproduction: `ChangeBatch::<usize>::new_from(17, 0)` followed by `is_empty()` returns `false`.

Severity: low — zero-valued updates are uncommon in practice, but the invariant that "compact entries are non-zero" is violated.

Fix: change the guard to `self.updates.len() > 1` → `self.updates.len() >= 1`, or handle the single-element case separately with a retain check.

## `timely/src/progress/broadcast.rs`

### `recv` logging pre-allocates with wrong variable

In `recv()` at line 120-121, the logging closure allocates:
```rust
let mut messages = Vec::with_capacity(changes.len());
let mut internal = Vec::with_capacity(changes.len());
```
`changes` is the *output accumulator*, not the received message (`recv_changes`).
Compare with `send()` at line 66-67 where `changes` correctly refers to the data being sent.

Severity: very low — wrong capacity hint in a logging-only path; no correctness impact.

## `timely/src/dataflow/operators/generic/notificator.rs`

### `OrderReversed` has inconsistent `PartialEq` and `Ord`, causing incorrect notification counts

`OrderReversed` derives `PartialEq` (compares both `element: Capability<T>` and `value: u64`) but implements `Ord` comparing only by `element.time()`.
`Capability<T>::PartialEq` additionally checks `Rc::ptr_eq` on the internal change batch.

In `next_count()` (line 342-348), the loop `while self.available.peek() == Some(&front)` uses `PartialEq` to merge same-time entries from the BinaryHeap.
This fails to merge entries that have:
* Different `value` fields (e.g., one consolidated with count 3, another directly inserted with count 1).
* Different `Rc` pointers in their `Capability` (capabilities from different cloning chains).

Reproduction scenario:
1. `make_available` consolidates two pending notifications for time T into `(T, count=2)` and pushes to `available`.
2. `notify_at_frontiered` adds `(T, count=1)` directly to `available`.
3. `next_count` pops `(T, 2)`, peeks at `(T, 1)`, comparison returns false (different `value`), returns `(cap, 2)` instead of `(cap, 3)`.

The next `next_count` call returns the leftover `(cap, 1)`, so the total is eventually correct but split across calls.

Severity: medium — notification counts are inaccurate.
Users relying on `count` in `for_each(|cap, count, _| ...)` may see split notifications for the same time.
Functional correctness of the dataflow is unaffected (all notifications are delivered), but the count semantic is broken.

Fix: change the `peek` comparison to compare only by time:
```rust
while self.available.peek().map(|x| x.element.time() == front.element.time()).unwrap_or(false) {
```

## `timely/src/worker.rs`

### `Config::from_matches` uses wrong default for `progress_mode`

`Config::from_matches` at line 115 uses `ProgressMode::Eager` as the fallback when `--progress-mode` is not specified:
```rust
let progress_mode = matches
    .opt_get_default("progress-mode", ProgressMode::Eager)?;
```

However, the `Default` impl for `ProgressMode` (line 64) is `Demand`:
```rust
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum ProgressMode {
    Eager,
    #[default]
    Demand,
}
```

This means `Config::thread()` and `Config::process(n)` use `Demand` (via `Config::default()`), but `execute_from_args` without `--progress-mode` uses `Eager`. The documentation explicitly recommends `Demand` as the safer default.

Reproduction: calling `execute_from_args(std::env::args(), ...)` without `--progress-mode` yields `Eager`, while `execute(Config::process(n), ...)` yields `Demand`.

Severity: low-medium — the two entry points silently use different progress modes. Users of `execute_from_args` get the less robust `Eager` mode by default, which risks saturating the system with progress messages.

Fix: change line 115 to use `ProgressMode::Demand` (or `ProgressMode::default()`) as the fallback:
```rust
let progress_mode = matches
    .opt_get_default("progress-mode", ProgressMode::default())?;
```

## `communication/src/allocator/zero_copy/allocator.rs`

### `receive()` uses `size_of::<MessageHeader>()` instead of `header.header_bytes()`

In `receive()` at line 270, the header is stripped from the payload using:
```rust
let _ = peel.extract_to(::std::mem::size_of::<MessageHeader>());
```

`MessageHeader` has 6 `usize` fields, so `size_of::<MessageHeader>()` is platform-dependent (48 on 64-bit, 24 on 32-bit).
The wire format always uses 6 `u64` values, so the correct strip size is `header.header_bytes()` which returns `size_of::<u64>() * 6 = 48` unconditionally.

Compare with `allocator_process.rs` line 197 which correctly uses:
```rust
let _ = peel.extract_to(header.header_bytes());
```

On 64-bit platforms the values coincide (48 = 48), so the bug is latent.
On 32-bit platforms, only 24 bytes would be stripped, leaving 24 bytes of header data mixed into the payload, corrupting every deserialized message received over TCP.

Reproduction: compile and run a multi-process timely computation on a 32-bit target. All inter-process messages will deserialize incorrectly.

Severity: low — 32-bit deployments are rare, and the two values coincide on the dominant 64-bit platform. The inconsistency with `allocator_process.rs` indicates the intent was to use `header.header_bytes()`.

Fix: change line 270 to:
```rust
let _ = peel.extract_to(header.header_bytes());
```
