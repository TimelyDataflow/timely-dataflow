# Sizing: two-type (builder/frozen) split of `PortConnectivity`

Branch `port_connectivity_typed`, built on `origin/port_connectivity_vec` (1a3a862a).
`cargo test -p timely` passes fully (unit, integration, doctests); the whole workspace
builds without warnings.

## Diff stat vs. base

```
 timely/src/dataflow/operators/generic/builder_raw.rs |  16 +--
 timely/src/dataflow/operators/generic/builder_rc.rs  |   8 +-
 timely/src/progress/operate.rs                       | 141 +++++++++----------
 timely/src/progress/reachability.rs                  |  15 +--
 timely/src/progress/subgraph.rs                      |  16 +--
 5 files changed, 82 insertions(+), 114 deletions(-)
```

Net **−32 lines**. `handles.rs` and `capability.rs` are untouched (see below).

## Deleted

- The `dirty: bool` field, including its maintenance logic in `add_port`
  (the "did this append stay sorted?" tracking) and its presence in the
  serde/columnar-serialized and logged representation.
- Both `debug_assert!(!self.dirty, ...)` guards in `iter_ports`/`get`.
- `is_consolidated()` and `consolidate()` on `PortConnectivity` (the merge logic
  survives, simplified, inside `PortConnectivityBuilder::freeze`, with no dirty
  check and no `retain` pass).
- The entire `Consolidate` extension trait on `Connectivity<TS>` and its impl (~20 lines).
- The contract sentence in the `Operate::initialize` doc ("must satisfy
  `Consolidate::is_consolidated`") — the return type now says it.
- The defense-in-depth block in `PerOperatorState::new` (subgraph.rs):
  `debug_assert!(is_consolidated)` + `consolidate()` + 3-line comment.
- `summary.consolidate()` + comment in `reachability::Builder::add_node`
  (it now accepts frozen input; non-canonical input is unrepresentable).
- `self.summary.consolidate()` in builder_raw's `initialize` (which also drops
  back to `self: Box<Self>` from `mut self`), and `internal_summary.consolidate()`
  in `Subgraph::initialize`.

## Added

- `PortConnectivityBuilder<TS>` (~55 lines with docs): `Default`, `insert`,
  `add_port` (plain push, empty summaries discarded), `freeze(self) -> PortConnectivity`
  (sort + merge), `FromIterator`. No reads; the only way out is `freeze`.
- `PortConnectivity::into_builder(self)` — needed for the builder_rc case.
- Freeze sites: **4** — builder_raw `build_typed` (freezes the accumulated
  per-input summaries once, so `initialize` is read-only), `Subgraph::initialize`
  (per-input builders frozen after summarizing), `summarize_outputs` in
  reachability.rs (per-location builders frozen at the end), and builder_rc
  `new_output_connection` (the re-freeze swap below).
- One behavioral nit in builder_raw `new_input_connection`: the port-bounds
  `assert!` moved into an `.inspect()` before collection, so it now also fires
  for out-of-range ports carrying *empty* antichains (previously filtered out
  before the assert). Strictly stricter; no caller trips it.

## The handles.rs shared-mutation case

This turned out to be a non-event in the diff: `handles.rs` and `capability.rs`
keep `Rc<RefCell<PortConnectivity<T::Summary>>>` (frozen) unchanged. The timing
works out: `InputCapability` reads (`delayed`, `valid_for_output`) happen only at
operator runtime, after construction, while `new_output_connection` mutations
happen only during construction. Rather than store a builder in the `Rc` (which
would force the runtime read path through a non-canonical-tolerant API,
reintroducing the temporal invariant in worse form), builder_rc **re-freezes in
place** on each mutation:

```rust
let mut shared = self.summaries[input].borrow_mut();
let mut builder = std::mem::take(&mut *shared).into_builder();
builder.add_port(new_output, entry);
*shared = builder.freeze();
```

This is the "freeze-and-swap" option, applied per mutation rather than at
`build()` (per-build swap is impossible anyway: the `Rc`'s pointee type is fixed
when handles are created). Honest cost: 5 lines + comment where one line stood,
an `into_builder` round-trip, and an O(ports) re-sort per output addition —
construction-time only, on tiny nearly-sorted vectors. Honest benefit: the shared
value is canonical at *every* instant, not just at read time, so even a hostile
interleaving (reading a capability summary mid-construction) sees a coherent value.

## Verdict

**Net simpler.** The two-type version is −32 lines *and* deletes every piece of
non-local reasoning: the dirty bit, the "reads require consolidated form" doc
contract, the two read-side debug_asserts, the defense-in-depth consolidate at
the `initialize` boundary, and the `Consolidate` extension trait. What it adds —
one builder type and four `freeze` calls — is all locally checkable: each freeze
site is exactly where accumulation provably ends, and the compiler enforces it.
The serialized/logged form also sheds the dirty bit. The one place the design
pushed back (builder_rc's shared `Rc<RefCell<...>>`) cost five lines of
freeze-and-swap rather than any new invariant. The single-type-with-dirty-bit
design is fewer *nominal* types but more *temporal* obligations; this version
trades a documented invariant for a type, and comes out smaller even in raw
line count.
