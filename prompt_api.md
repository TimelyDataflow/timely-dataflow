# API consistency audit tracker

## Crates and areas

### `container/` traits — DONE
* [x] `Accountable`, `DrainContainer`, `SizableContainer`, `PushInto`, `ContainerBuilder`

### `bytes/` types — DONE
* [x] `Bytes` vs `BytesMut` API symmetry — 1 inconsistency found

### `communication/` traits and types — DONE
* [x] `Push`/`Pull` trait and implementations
* [x] `Allocate` trait and implementations
* [x] `Bytesable` trait

### `timely/` — progress types — DONE
* [x] `ChangeBatch` vs `MutableAntichain` vs `Antichain` — 3 inconsistencies found

### `timely/` — dataflow/operators: `core` vs `vec` — DONE
* [x] Input / UnorderedInput
* [x] Exchange, Filter, Map, Partition, ToStream
* [x] Remaining operator pairs (Branch, Delay, Broadcast, etc.)
* 6 inconsistencies found

### `timely/` — dataflow/operators/generic — DONE
* [x] Builder variants (raw, rc, ref)
* [x] Operator construction APIs

### `timely/` — dataflow/channels — DONE
* [x] Pushers and pullers across channel types

### `timely/` — top-level — DONE
* [x] Worker, Config, execution entry points
* [x] Stream type methods
* [x] Input Handle — 1 inconsistency found
