# `timely_communication::chain` design notes

Per-writer, forward-linked, compacting chains: an intra-process all-reduce intended
to eventually replace the Progcaster's intra-process leg. Lives at
`communication/src/chain.rs`.

## Contract

- A `Chain<T>` has one writer (`Writer<T>`, created by `Chain::new`) and any number
  of readers (`Chain::reader`). Readers created later observe only later sends.
- `Writer::send(v)` commits an atom. Every reader folds, exactly once, every atom
  committed after its registration. Atoms may be merged with adjacent atoms (never
  split) via `T: Chainable`, a commutative monoid (`fn merge_from(&mut self, &Self)`).
  Commutativity is required because multiple chains and multiple readers impose no
  cross-atom ordering.
- Live state is bounded by `O(#readers)`, independent of send count, provided
  readers occasionally `recv` (every `recv` and every reader drop compacts).
- `Mesh<T>::new(writers)` bundles `W` chains: per-worker `Writer<T>` handles plus
  `MeshReader<T>` handles (`Mesh::reader`) that sweep all chains. Network threads
  are just more readers; a receiving network thread is one more writer.
- `Reader::recv(&mut self, out: &mut T)` folds; `recv_with(f)` hands each atom to
  the caller; `is_caught_up()` is an O(1) peek (pin ptr == newest ptr).

## Structure

Forward links (old → new). Each node: `holders: AtomicUsize` (count of readers
pinned there; a pin means "I have folded everything up to and including this node")
and a payload `(value: Option<T>, next: Option<Arc<Node>>)` behind ONE `RwLock`, so
walkers snapshot a consistent pair and compactors mutate both atomically. The chain
holds `newest: Mutex<Arc<Node>>` (writer's append point) and `oldest:
Mutex<Arc<Node>>` (compaction sweep origin; invariant: every pin is at or after
`oldest`). Pins are RAII (`Held`), as in the prototype.

Writer fast path: under the `newest` mutex and the newest node's payload write
lock, if `holders == 0` merge in place (zero allocation); else allocate, link
`old.next = new`, swap the pointer. The `holders` check happens under the payload
*write* lock, which excludes concurrent pinning (see deviations).

Reader walk: from its pin, hand-over-hand — pin and fold each successor under that
successor's payload read lock, then drop the old pin. The fold frontier is thus
always a pinned node, which is what makes compaction's `holders` checks sufficient.
A node has `next == None` iff it is the chain's newest, which terminates the walk.

Garbage collection of the prefix is by `Arc` refcounts: forward links mean nothing
points backward, so once `oldest` advances past an unpinned head node the abandoned
prefix simply deallocates.

## Compaction rule and safety argument

`a` may absorb its successor `b` (merge `b.value` into `a.value`, set
`a.next = b.next`) iff `a.holders == 0` AND `b.holders == 0` AND `b` is not the
newest node. Checks are made under both payload write locks (taken older-before-
newer), excluding concurrent pinning (pins are taken under a payload read lock, or
under the `newest` mutex for the newest node).

- No lost atoms: values move only backwards, into the unique live predecessor. A
  reader that has not folded `b` has its (pinned) fold frontier strictly before
  `b`; `a.holders == 0` excludes a frontier exactly at `a`, and every path from an
  earlier pin to `b` passes through `a`, where the value now lives.
- No double folds: a reader that has folded `b` is pinned at or after `b` and never
  revisits `a`; pinned-at-`b` readers are excluded by `b.holders == 0`.
- Pins never dangle: pinned nodes are never absorbed (nor merged into), so a pin's
  `next` always leads into the live chain.
- The newest node is never absorbed (the writer merges into it instead), so the
  `newest` pointer never dangles. The sweep uses a snapshot of `newest`; the true
  newest is at or after the snapshot, so never absorbed.

Compaction runs as a full sweep from `oldest` at the start of every `recv` and on
every `Reader::drop`: advance `oldest` past unpinned head nodes (folded by everyone,
unpinnable thereafter), then merge every adjacent unpinned pair. Sweeps are
serialized by holding the `oldest` mutex throughout (two interleaved sweeps could
otherwise drain a value into a just-unlinked node).

## Lock ordering

1. `oldest` mutex (held across a whole sweep),
2. `newest` mutex,
3. node payload `RwLock`s in chain order (older before newer), at most two at once.

Writer: (2) then (3, newest node only). Reader registration: (2) only. Walk: (3),
one at a time. Sweep: (1), then (2) briefly (released before node locks), then (3)
pairwise in order. No cycle.

## Deviations from the brief

1. **Compaction never bypasses pinned nodes** (brief: "a pinned `b` may be bypassed
   safely"). Counterexample to the brief's rule: chain `a → p → c` with reader R
   pinned at `p`. Bypass `p` (`a.next = c`, `p.next` frozen at `c`); then absorb
   `c` into `a` (`a.holders == 0` holds). R resumes via `p.next = c`, but `c`'s
   value has moved to `a`, behind R's fold frontier — a lost atom. Frozen `next`
   pointers of bypassed pinned nodes are side entrances into the chain that the
   `a.holders` check cannot see. Requiring `b.holders == 0` as well removes them.

2. **An `oldest` pointer and full sweeps replace "absorb pairs the walk crosses".**
   With never-bypass, a reader's own walk cannot compact the region behind its pin,
   and with forward links a node can only be unlinked by its predecessor — found
   only by walking from behind. Without this, the segment between a laggard and the
   active readers grows by one node per catch-up (each catch-up pins the newest
   node, forcing the next send to allocate; the abandoned pin then sits unreachable
   to every active walk). The `oldest`-origin sweep on every `recv` is strictly
   stronger self-healing than the brief's crossed-pairs rule and is what actually
   delivers the O(#readers) bound (observed: steady length 4 = #readers + 2 with
   one laggard and one active reader, over 10k sends).

3. **Pins are bumped under the node's payload read lock** (plus the `newest` mutex
   for registration), not under the newest-pointer lock as the brief sketched. The
   writer and the compactor re-check `holders` under the payload *write* lock,
   giving the same exclusion. This finer grain is needed because mid-walk readers
   pin hand-over-hand on interior nodes the newest-pointer lock says nothing about;
   hand-over-hand pinning in turn is what stops a sweep from outrunning a walker
   (absorbing a node the walker has passed but whose successor it has not folded).

4. **`recv_with` invokes the callback under a node's payload read lock**; it must
   not reenter the same chain. Documented on the method.

5. `Reader<T: Chainable>` carries the bound on the struct so `Drop` (which cannot
   add bounds) can run the compaction sweep.

## Test results

`cargo test -p timely_communication`: 19 passed (11 new chain tests, including the
randomized stress test with 3 writer threads, 3 active reader threads, and one
laggard over 50 phases, asserting per-chain length ≤ #readers + 2 at quiescent
checkpoints and exact final totals; also a fast-path test asserting chain length is
exactly 2 — the pinned caught-up node plus one accumulating newest — after N sends
with no recv). `cargo test -p timely`: all pass, untouched. Clippy (workspace lint
set): no findings on the new module.
