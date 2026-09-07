# Transport and progress engineering assessment

This assessment covers the current six-crate workspace, the columnar transport
experiment, and the concurrent progress structure proposed in pull request
807. Measurements below were made on an Apple Silicon host with four workers,
release builds, 5,000 fixed-width 64-byte records per worker per logical round,
and all-to-all routing. Allocation counts include `alloc` and `realloc`; bytes
are requested bytes, not live-set size.

## Memory-footprint constraint

Transport scratch space must obey this quiescent-state invariant:

> Quiescent retained transport capacity is bounded per worker or allocator,
> independent of the number of logical channels and destination workers.

A fixed per-channel pool does not satisfy this property, even when its local
bound looks small. An exchange distributor owns one builder per destination.
Retaining one preferred-size container in every builder can therefore scale as
`channels × source workers × destination workers × container capacity`. At
10,000 exchange channels, 100 workers, and 1 MiB containers, the theoretical
process-wide bound is 100 TB. Sixteen-container input or thread-channel pools
scale as `channels × workers × 16 × capacity` and are also unacceptable.

The zero-copy allocator is the appropriate place to retain a process-wide,
budgeted set of compact byte slabs. Typed column scratch space should disappear
when its logical channel becomes quiescent.

## Revised transport tranche

- `timely_container::columnar::{ColumnarContainer, ColumnarBuilder}` (also
  re-exported from `timely::container`) promotes
  the former example-only implementation into supported infrastructure.
  Binary receivers retain a view into compact communication `Bytes` rather
  than reconstructing owned rows.
- A columnar builder may recycle returned typed columns only while its current
  sequence is active. The final `finish()` call and `relax()` both release
  `current`, returned containers, and the bounded transient spare list.
- The proposed generic changes to `CapacityContainerBuilder`,
  `InputHandleCore`, and the thread-local allocator were removed. Their
  seemingly small per-instance bounds multiplied by channel and worker counts.
- Tests include 10,000 independently activated and quiesced builders and assert
  that none retains a current allocation or pooled container.
- `timely/examples/transport_alloc.rs` supplies a repeatable typed/binary ×
  vector/columnar matrix, allocation-size histogram, warmup, and record-count
  validation.

### Allocation versus retention

The initial recycling experiment found a real allocation mechanism. For one
million records, binary columnar fell from 257.6 to 26.4 requested bytes/record
and allocation calls fell 9.7x. Repeated geometric growth fell from 126.8 MB to
3.9 MB in the 4–64 KiB bucket and from 93.1 MB to 3.2 MB in the 64–256 KiB
bucket. The exact short-run throughput moved from 64.2 to 84.4 million
records/second.

That result was not free: it converted allocation churn into long-lived
per-destination typed column capacity. On upstream 0.31 the retained variant
reached 9.58 allocated bytes/record and 95.9 million records/second after
warmup, but violated the quiescent-state invariant above. Those numbers are
recorded as a rejected point in the tradeoff space, not as the behavior of the
revised PR.

With all per-channel retention removed, the same two-million-record matrix
produced:

| Transport | Container | allocated bytes/record | records/s |
|---|---:|---:|---:|
| typed | `Vec<Record>` | 68.4 | 309M |
| binary | `Vec<Record>` | 80.7 | 98M |
| typed | columnar | 472.3 | 85M |
| binary | columnar | 480.3 | 64M |
| none | direct columnar builder | 236.0 | 134M |

The fixed-width columnar microbenchmark is now deliberately allocation-heavy:
source and exchange scratch columns are regrown after each quiescence boundary.
It demonstrates that the 9.8x allocation reduction was purchased with retained
state. Columnar transport can still be useful for variable-width records,
receiver-side borrowed access, and avoiding reconstruction of owned rows, but
the fixed-width result is not a throughput recommendation.

A future recycling design should use a worker-wide byte budget shared among
active channels, rather than a count embedded in each builder. Doing that well
requires a capacity-reporting/reinitialization contract for generic containers;
it is deferred rather than hidden behind an unsafe aggregate memory bound.

## Pull request 807: progress exchange

The PR's central diagnosis is right: broadcast MPSC queues make each sender
clone progress batches for every reader, preserve obsolete intermediate state,
and charge a laggard for the full history rather than the consolidated net.
Its shared compacting chain demonstrably protects laggards and bounds backlog,
but the single shared head moves the cost to the healthy case. The PR's own
measurements show a 2–7x synthetic send-path loss and a 6.6x loss at eight
workers in the progress-heavy `event_driven` workload, while data-heavy
PageRank is approximately unchanged. That agrees with the reported experience
that no overall improvement was measurable.

My disposition would also be “keep as an experiment, do not make it the sole
default.” It optimizes an important failure mode, but forces every healthy
worker through a globally written cache line and nested lock protocol. It is a
resource-governance improvement, not yet a throughput improvement.

### A more promising shape

Use a bounded, hierarchical combining tree rather than either W broadcast
queues or one global chain:

1. Each worker publishes into a single-producer local delta slot/log, with a
   monotonically increasing generation. It never clones per reader.
2. One combiner per small socket-local group (for example 4–8 workers) drains
   changed generations into a consolidated group accumulator. Writers use a
   `try_lock`; on contention they retain and consolidate into their local slot
   rather than waiting on a global head.
3. Group accumulators feed a second-level accumulator only when their net
   changes. Readers track a generation per group and fold the latest
   consolidated snapshots.
4. Put an explicit byte/entry budget on every local slot. A lagging publisher
   consolidates more aggressively; a lagging reader does not prevent writers
   or other readers from reclaiming historical nodes.

This gives cross-writer cancellation within groups, reduces shared-cacheline
fan-in from W to roughly the group size, and makes laggard work proportional to
current consolidated state rather than elapsed sends. It does change the
proof obligation: publication must expose an atomic snapshot/generation pair,
and reclamation must wait until all readers have acknowledged that generation.
An epoch or two-buffer seqlock cell is simpler to audit than a mutable linked
chain.

Two useful variants should be benchmarked before implementation:

- **Striped ledger by topology, not by key.** Each send remains atomic and goes
  to the writer's group stripe; a reader consolidates the small set of stripes.
  This preserves send atomicity while allowing cross-writer cancellation inside
  each stripe.
- **RCU snapshot plus delta inbox.** Writers append small deltas to bounded
  per-writer SPSC rings. A combiner periodically publishes an immutable
  consolidated `Arc` snapshot. Readers normally clone one snapshot and process
  only deltas newer than its generation. A laggard jumps to a newer snapshot
  instead of replaying history.

The benchmark acceptance criteria should be stated as a Pareto frontier:
healthy progress-heavy throughput, p99 send/receive latency, retained bytes
with one unread worker, and catch-up work after 1/16/1024 scheduling rounds.
A single throughput number hides the protection that motivated the structure.

## Broader engineering assessment

The codebase has unusually clean conceptual seams: bytes, containers,
communication, progress, scheduling, and operator construction are separate
crates/modules; the `Push<&mut Option<T>>` ownership slot is a strong and
underused abstraction; and progress correctness is largely isolated from data
representation. Tests are small and generally exercise semantic contracts.

The highest complexity is concentrated in `progress/reachability.rs`,
`progress/frontier.rs`, `progress/subgraph.rs`, `worker.rs`, and the generic
operator builders. Their complexity is mostly inherent, but several incidental
costs can be removed:

- Consolidate the three generic builder implementations (`builder_raw`,
  `builder_rc`, and `builder_ref`) around one internal wiring/state machine.
  Keep the public APIs as adapters; today duplicated frontier, capability, and
  shutdown bookkeeping makes changes harder to audit.
- Split `progress/subgraph.rs` into topology construction, runtime progress
  exchange, and scheduling/activation state. This would make it possible to
  replace the progress medium without editing the progress calculus.
- Specify `ContainerBuilder::relax` as a quiescence and memory-reclamation
  boundary. Any future retention should be charged to an explicit worker-wide
  byte budget, not an implicit per-builder count.
- Make resource-return behavior an explicit allocator capability. `Process`
  cannot return typed resources to a source; binary allocators return the
  typed input immediately; thread-local channels can return consumed values.
  Encoding this distinction in types or diagnostics would prevent container
  choices whose recycling assumptions cannot be met.
- Separate benchmark-only concurrent structures from exported communication
  primitives. `communication/src/chain.rs` is currently not exported or wired
  into `Progcaster`; its presence in the source tree otherwise suggests a
  supported facility that does not exist.

## Deferred work

- Do not skip `columnar::Stash::try_from_bytes` validation by default. The
  receive path is already byte-backed; unchecked construction would weaken a
  network trust boundary for little demonstrated gain.
- A bidirectional typed process channel could recycle containers, but routing a
  returned generic `T` to its original sender requires per-source receive lanes
  or protocol metadata. That is a larger allocator redesign and should be
  measured against simply using `ProcessBinary`.
- The next columnar experiment should use variable-width strings and nested
  records. Fixed-width rows establish recycling behavior, but do not quantify
  the layout's main cache-locality and allocation-count advantage.
- Run PR 807 and the hierarchical variants on a many-core, multi-socket Linux
  host. The current single-socket Apple Silicon result is enough to reject a
  universal default, not enough to reject the laggard-protection design goal.
