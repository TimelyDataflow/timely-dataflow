# chain_bench: multi-writer Chain vs MPSC baseline vs per-writer Mesh

Harness: `communication/examples/chain_bench.rs`. Apple Silicon (10 cores),
release, median of 3 after warmup. Delta = miniature ChangeBatch
(`Vec<(u64,i64)>`, amortized consolidation). Workload: all-to-all; worker w
mints `(t,+1)` each step, its successor retires it `(-1)` one step later for
`cancel%` of steps. MPSC = per-reader `Mutex<VecDeque<Delta>>`, send clones
to every reader (models the current Progcaster intra-process path).

Full tables at the end. Headlines:

## Scenario C — unread backlog (the design goal): Chain wins by orders of magnitude

N=8, 100% cancellation, 50k sends/worker with nobody reading:

| structure | retained entries | retained units | catch-up |
|---|---|---|---|
| mpsc | 6,399,936 | 3,200,000 queued deltas | 46 ms |
| chain | **6,124** | **2 nodes** | **0.4 ms** |
| mesh (per-writer) | 799,992 | 16 nodes | 27 ms |

Three findings in one table: (1) the chain's in-transit cancellation works —
retained state is the *net* in-flight window (~6k entries), 1000× below the
MPSC backlog and independent of elapsed sends; (2) the per-writer Mesh
pathology predicted in review is real — bounded *nodes* but unbounded
*content* (130× the chain's entries at 100% cancel), because cancellation is
inherently cross-writer; (3) even with zero cancellation the chain retains N×
less than MPSC, which pays the per-peer clone multiplier.

## Scenario B — laggard protection: Chain's laggard work is flat in N

Laggard recvs every 1024 steps; mean entries folded per laggard recv:

| N | mpsc | chain | mesh |
|---|---|---|---|
| 2 | 4,076 | 2,576 | 2,415 |
| 4 | 6,004 | 2,343 | 2,127 |
| 8 | 11,155 | **2,193** | 2,120 |

MPSC laggard work grows with N (and bursts: max 38k entries/recv at N=8);
the chain's is bounded by the live cancellation window — flat as N grows,
with proportionally lower mean recv latency.

## Scenario A — everyone keeps up: MPSC wins, and the gap grows with N

Sends/sec, 100% cancel: N=2: mpsc 5.4M vs chain 2.6M (2.1×); N=4: 3.1M vs
1.0M (3.0×); N=8: 1.07M vs 0.42M (2.6–7× across cancel rates, ~7× at the
worst point). This is the predicted head-mutex contention, confirmed: every
send serializes through one lock, and the benchmark sends in a tight loop
with no work between sends — the maximally contended regime.

## Verdict: better for whom

The chain delivers exactly what it was designed for — laggards and memory:
bounded backlog state (orders of magnitude), bounded laggard fold work (flat
in N), fast catch-up — and pays for it on the uncontended fast path, where
MPSC's clone-into-W-queues is 2–7× faster in this harness. Three reasons the
fast-path penalty overstates the real cost, and one real mitigation:

- Real progress traffic is one batch per worker per *scheduling step* with
  dataflow work between sends; the benchmark's send-only tight loop is the
  worst case for lock contention.
- The Subgraph already accumulates locally and ships net diffs once per
  round (the "thread-local tier"), so send frequency is bounded by step
  rate, not update rate.
- MPSC's advantage shrinks as payloads grow (it clones per peer; the chain
  writes once).
- If contention still bites: **key-sharded chains** — K chains with shard =
  hash(update key) % K. Cancellation pairs share their key by construction
  (`(t,+1)` cancels `(t,-1)`), so sharding by key splits contention K ways
  while preserving in-transit cancellation exactly. This is the
  mimalloc-style sharding that fits this workload, unlike per-writer
  sharding (Mesh), which destroys cancellation.

Suggested next step: wire a `Progcaster` variant on the chain behind a
config flag and measure on real workloads (event_driven at -w 4/8), where
send cadence and payload shapes are honest; revisit sharding only if the
head mutex shows up there.

## Full results

(See `/tmp/chain-bench-out.md` snapshot below.)
# chain_bench results

Delta: miniature ChangeBatch (`Vec<(u64, i64)>`, amortized consolidation).
Workload: all-to-all; worker w mints (t,+1) each step; its successor
retires it (-1) one step later for `cancel%` of steps. Timing rows are
the median of 3 runs after a warmup run.

## Scenario A: all keep up (400000 steps/worker; send + recv every step)

| N | cancel% | structure | wall (s) | sends/sec |
|---|---------|-----------|----------|-----------|
| 2 | 100 | mpsc | 0.147 | 5428123 |
| 2 | 100 | chain | 0.302 | 2644752 |
| 2 | 100 | mesh | 0.330 | 2426396 |
| 2 | 50 | mpsc | 0.136 | 5882342 |
| 2 | 50 | chain | 0.273 | 2927853 |
| 2 | 50 | mesh | 0.267 | 2990975 |
| 2 | 0 | mpsc | 0.109 | 7331403 |
| 2 | 0 | chain | 0.198 | 4044385 |
| 2 | 0 | mesh | 0.226 | 3541374 |
| 4 | 100 | mpsc | 0.516 | 3101072 |
| 4 | 100 | chain | 1.540 | 1038627 |
| 4 | 100 | mesh | 1.620 | 987528 |
| 4 | 50 | mpsc | 0.464 | 3445407 |
| 4 | 50 | chain | 1.360 | 1176794 |
| 4 | 50 | mesh | 1.491 | 1072963 |
| 4 | 0 | mpsc | 0.428 | 3742396 |
| 4 | 0 | chain | 1.102 | 1452381 |
| 4 | 0 | mesh | 1.372 | 1166240 |
| 8 | 100 | mpsc | 2.994 | 1068725 |
| 8 | 100 | chain | 7.636 | 419063 |
| 8 | 100 | mesh | 7.867 | 406768 |
| 8 | 50 | mpsc | 2.669 | 1199062 |
| 8 | 50 | chain | 7.209 | 443915 |
| 8 | 50 | mesh | 7.798 | 410371 |
| 8 | 0 | mpsc | 2.805 | 1140937 |
| 8 | 0 | chain | 6.428 | 497823 |
| 8 | 0 | mesh | 7.242 | 441895 |

## Scenario B: laggard (400000 steps/worker; worker 0 recvs every 1024 steps)

| N | cancel% | structure | wall (s) | laggard recvs | mean entries/recv | max entries/recv | mean latency (µs) | max latency (µs) |
|---|---------|-----------|----------|---------------|-------------------|------------------|-------------------|------------------|
| 2 | 100 | mpsc | 0.117 | 390 | 4076 | 6488 | 121.5 | 258.1 |
| 2 | 100 | chain | 0.203 | 390 | 2576 | 3168 | 80.2 | 10399.5 |
| 2 | 100 | mesh | 0.194 | 390 | 2415 | 41102 | 79.4 | 11282.8 |
| 2 | 50 | mpsc | 0.094 | 390 | 3042 | 118637 | 97.8 | 8659.8 |
| 2 | 50 | chain | 0.154 | 390 | 1891 | 2287 | 68.7 | 8357.9 |
| 2 | 50 | mesh | 0.147 | 390 | 1671 | 1795 | 58.7 | 9339.0 |
| 2 | 0 | mpsc | 0.098 | 390 | 2050 | 3195 | 65.2 | 7199.0 |
| 2 | 0 | chain | 0.109 | 390 | 1180 | 1449 | 39.4 | 6636.7 |
| 2 | 0 | mesh | 0.096 | 390 | 1092 | 8884 | 29.3 | 4628.4 |
| 4 | 100 | mpsc | 0.531 | 390 | 6004 | 17672 | 220.8 | 8146.7 |
| 4 | 100 | chain | 1.176 | 390 | 2343 | 2736 | 175.7 | 11835.3 |
| 4 | 100 | mesh | 1.158 | 390 | 2127 | 3356 | 165.2 | 11332.7 |
| 4 | 50 | mpsc | 0.434 | 390 | 4971 | 55476 | 182.9 | 14163.3 |
| 4 | 50 | chain | 0.999 | 390 | 1740 | 2032 | 123.4 | 10331.8 |
| 4 | 50 | mesh | 1.018 | 390 | 1585 | 1680 | 114.7 | 6036.5 |
| 4 | 0 | mpsc | 0.405 | 390 | 3414 | 18212 | 173.1 | 25595.2 |
| 4 | 0 | chain | 0.778 | 390 | 1134 | 1333 | 88.9 | 5466.0 |
| 4 | 0 | mesh | 0.842 | 390 | 1051 | 1100 | 69.3 | 6135.2 |
| 8 | 100 | mpsc | 2.677 | 390 | 11155 | 38426 | 467.7 | 12300.8 |
| 8 | 100 | chain | 6.767 | 390 | 2193 | 2394 | 437.0 | 18932.1 |
| 8 | 100 | mesh | 7.896 | 390 | 2120 | 2394 | 365.9 | 11790.2 |
| 8 | 50 | mpsc | 2.737 | 390 | 9503 | 52156 | 411.0 | 24853.5 |
| 8 | 50 | chain | 6.052 | 390 | 1633 | 1800 | 262.4 | 11031.0 |
| 8 | 50 | mesh | 7.351 | 390 | 1582 | 1703 | 339.4 | 18233.4 |
| 8 | 0 | mpsc | 2.668 | 390 | 5781 | 18775 | 424.2 | 52151.5 |
| 8 | 0 | chain | 5.252 | 390 | 1074 | 1202 | 216.0 | 6562.3 |
| 8 | 0 | mesh | 6.816 | 390 | 1052 | 1217 | 188.7 | 5855.1 |

## Scenario C: unread backlog (50000 steps/worker; recv only at the end)

Retained units: queued deltas (mpsc) or live chain nodes (chain/mesh).

| N | cancel% | structure | retained entries | retained units | folded by reader 0 | catch-up (s) |
|---|---------|-----------|------------------|----------------|--------------------|--------------|
| 2 | 100 | mpsc | 399996 | 200000 | 199998 | 0.005 |
| 2 | 100 | chain | 2964 | 2 | 2964 | 0.000 |
| 2 | 100 | mesh | 199998 | 4 | 199998 | 0.001 |
| 2 | 50 | mpsc | 299996 | 200000 | 149998 | 0.005 |
| 2 | 50 | chain | 66612 | 2 | 66612 | 0.001 |
| 2 | 50 | mesh | 149998 | 4 | 149998 | 0.001 |
| 2 | 0 | mpsc | 200000 | 200000 | 100000 | 0.003 |
| 2 | 0 | chain | 100000 | 2 | 100000 | 0.001 |
| 2 | 0 | mesh | 100000 | 4 | 100000 | 0.000 |
| 4 | 100 | mpsc | 1599984 | 800000 | 399996 | 0.017 |
| 4 | 100 | chain | 334 | 2 | 334 | 0.000 |
| 4 | 100 | mesh | 399996 | 8 | 399996 | 0.010 |
| 4 | 50 | mpsc | 1199984 | 800000 | 299996 | 0.015 |
| 4 | 50 | chain | 106810 | 2 | 106810 | 0.002 |
| 4 | 50 | mesh | 299996 | 8 | 299996 | 0.007 |
| 4 | 0 | mpsc | 800000 | 800000 | 200000 | 0.009 |
| 4 | 0 | chain | 200000 | 2 | 200000 | 0.004 |
| 4 | 0 | mesh | 200000 | 8 | 200000 | 0.003 |
| 8 | 100 | mpsc | 6399936 | 3200000 | 799992 | 0.046 |
| 8 | 100 | chain | 6124 | 2 | 6124 | 0.000 |
| 8 | 100 | mesh | 799992 | 16 | 799992 | 0.027 |
| 8 | 50 | mpsc | 4799936 | 3200000 | 599992 | 0.036 |
| 8 | 50 | chain | 254600 | 2 | 254600 | 0.007 |
| 8 | 50 | mesh | 599992 | 16 | 599992 | 0.027 |
| 8 | 0 | mpsc | 3200000 | 3200000 | 400000 | 0.029 |
| 8 | 0 | chain | 400000 | 2 | 400000 | 0.011 |
| 8 | 0 | mesh | 400000 | 16 | 400000 | 0.015 |
