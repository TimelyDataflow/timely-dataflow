# Silent error swallowing audit log

## Findings by theme

### 1. Infinite spin-loop on failed header parse

**Severity: high (bug)**

Both `allocator.rs` and `allocator_process.rs` have a `while !bytes.is_empty()` loop that parses message headers.
When `MessageHeader::try_read` returns `None` (partial/corrupted header), the `else` branch prints to stdout but never advances `bytes` or breaks.
This creates an infinite busy-loop consuming 100% CPU.

* `communication/src/allocator/zero_copy/allocator.rs:291-293`
* `communication/src/allocator/zero_copy/allocator_process.rs:218-220`

The comment "We expect that `bytes` contains an integral number of messages" indicates this was considered unreachable, but the `else` branch exists specifically for the case where it isn't.
A `break` or `panic!` would be more appropriate than an infinite spin with `println!`.

### 2. Dropped event notification on channel send failure

**Severity: medium**

* `communication/src/allocator/counters.rs:99` — `let _ = self.events.send(self.index)`.
  The event-notification channel tells the receiving worker that a message arrived on a specific channel.
  If the send fails (receiver dropped), the message is delivered but the consumer is never notified.
  During normal operation this could cause messages to sit unprocessed until the next poll cycle.
  The code has a TODO comment acknowledging this: "Perhaps this shouldn't be a fatal error (e.g. in shutdown)."

### 3. Cycle detection only prints to stdout

**Severity: medium**

* `timely/src/progress/reachability.rs:200-203` — `Builder::build()` detects cycles without timestamp increment but only calls `println!` and continues.
  A cycle without timestamp increment is a liveness issue that will cause the computation to deadlock.
  The function signature returns `(Tracker<T>, Connectivity<T::Summary>)` with no `Result`, so it cannot propagate the error.
  Printing to stdout (not stderr) means the warning is easily missed.

### 4. Message send failures silently discarded during push

**Severity: low**

* `communication/src/allocator/process.rs:215` — `let _ = self.target.send(element)`.
  The comment explains: "The remote endpoint could be shut down, and so it is not fundamentally an error to fail to send."
  Messages are silently lost when the receiver is dropped.
  Intentional for graceful shutdown, but no logging or metric tracks dropped messages.

* `timely/src/dataflow/operators/core/capture/event.rs:41` — `let _ = self.send(event)`.
  Comment: "An Err(x) result just means 'data not accepted' most likely because the receiver is gone."
  Same pattern — intentional but unobservable.

### 5. `try_recv().ok()` conflates empty and disconnected

**Severity: low**

* `communication/src/allocator/process.rs:229` — `self.source.try_recv().ok()`.
  `TryRecvError::Empty` (no message available) and `TryRecvError::Disconnected` (sender dropped) are both mapped to `None`.
  The caller cannot distinguish "temporarily empty" from "permanently dead."
  In practice, peer departure is detected through other mechanisms.

### 6. Silent data drop for past-bound channels

**Severity: low**

* `communication/src/allocator/zero_copy/allocator.rs:278-284`
* `communication/src/allocator/zero_copy/allocator_process.rs:205-211`

When data arrives for a channel whose ID is at or below `channel_id_bound` (already allocated and dropped), the data is silently discarded.
Intentional for shutdown, but could mask messages routed to wrong channels.

### 7. Buffered channel data dropped on removal

**Severity: low**

* `communication/src/allocator/zero_copy/allocator.rs:240-241`
* `communication/src/allocator/zero_copy/allocator_process.rs:169-172`

When a channel is removed from `to_local`, any remaining buffered data in its `VecDeque<Bytes>` is dropped.
A commented-out `assert!(dropped.borrow().is_empty())` shows this was once checked.
Intentional when dataflows are forcibly dropped.

### 8. Unknown channel activations silently ignored

**Severity: low**

* `timely/src/worker.rs:367-371` — `if let Some(path) = paths.get(&channel)` ignores channels with no registered path.
  Has a TODO: "This is a sloppy way to deal with channels that may not be alloc'd."
  Could mask channel routing bugs, though in practice it handles transient state during startup/shutdown.

### 9. `activate_after` ignores delay when timer is None

**Severity: low**

* `timely/src/scheduling/activate.rs:80-92` — When `self.timer` is `None`, `activate_after` ignores the delay and activates immediately.
  Changes call semantics in a way the caller may not expect, but this is consistent with the `Worker` construction model.

### Non-findings

The following patterns were found but are intentional and safe:

* `logger.as_mut().map(|l| l.log(...))` — Conditional logging throughout the codebase. The discarded `Option<()>` carries no error information.
* `.unwrap_or_default()` on `stash.take()` in `container/src/lib.rs:213` — Correct fallback for optional recycled buffer.
* `.unwrap_or(false)` on `peek()` comparisons in `reachability.rs:648` — Standard loop termination.
* `Rc::try_unwrap` failure in `capture/event.rs:104` — Iterative drop pattern for linked lists.
* `Weak::upgrade()` returning `None` in `sequence.rs:140,199` — Standard cleanup when owner is dropped.
* `update_iter` return values discarded in frontier constructors — Changes during construction are not useful.
