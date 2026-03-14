# Operator fusion

When building dataflows, users often compose many small operators: a `map` followed by a `filter`, a `flat_map`, another `map`, and finally a `probe`.
Each operator is a separate node in the progress tracking graph, with its own `SharedProgress` handle, pointstamp accounting, and scheduling overhead.
For long pipelines, this overhead dominates actual computation.

Operator fusion detects groups of operators that can be scheduled as a single unit, hiding intermediate nodes from the reachability tracker.
This section explains how fusion works and why it preserves correctness.

## Which operators fuse

Fusion applies to operators connected by pipeline (thread-local) channels where the group's internal progress tracking can be collapsed without losing information.
An operator is *fusible* if:

* It does not observe frontiers (`notify == false`).
  Frontier-observing operators buffer data until they receive a notification that a timestamp is complete.
  Fusing them would require propagating frontiers within the group, which the scheduler does not do.
* All (input, output) pairs in its internal summary are the identity.
  Non-identity summaries (like the feedback operator's `Product(0, 1)`) require per-member timestamp transformation that the group's aggregate reporting does not support.
* It has an operator implementation (not already tombstoned).

An edge between two fusible operators is *fusible* if the target uses pipeline pact on the corresponding input port.
Exchange or broadcast pacts route data through inter-worker channels that the group scheduler cannot intercept.

Operators connected by fusible edges are grouped using union-find.
Groups with fewer members than a configurable threshold (`fuse_chain_length`, default 2) are left alone.
There is no restriction on fan-in or fan-out: diamonds, concatenations, and branches all fuse.

## How a fused group presents to the subgraph

A fused group replaces its member operators with a single `GroupScheduler` installed at the representative slot (the lowest-indexed member).
All other members become tombstones.

The group exposes:

* **Group inputs**: member input ports that receive edges from outside the group.
* **Group outputs**: member output ports that send edges outside the group, or that have no outgoing edges (their capabilities still need tracking).

The subgraph's `edge_stash` is rewritten: internal edges are removed, incoming edges are retargeted to the representative's group input ports, and outgoing edges are sourced from the representative's group output ports.

## Scheduling

Members are executed in topological order, computed by Kahn's algorithm over internal edges.
This guarantees that data pushed by a producer through a pipeline channel is available to its consumer when the consumer runs.

The physical pipeline channels between members are established during operator construction and are unaffected by fusion.
Only the progress tracking layer changes.

### Activation forwarding

Pipeline channels activate the original target operator when data arrives.
After fusion, the target may be a tombstone.
Each tombstone records a `forward_to` field pointing to the group representative.
The subgraph's scheduling loop checks this field and redirects the activation.

## Why the fused group reports correct progress

The key insight is that because all members have identity summaries, a capability at any member's output port at timestamp `t` implies the same timestamp `t` at every reachable group output.
The timestamp does not change along any internal path.

### Consumeds and produceds

The group reports consumeds only for group input ports and produceds only for group output ports.
Intermediate consumeds and produceds (data passing between members through internal pipeline channels) would cancel in the reachability tracker: a member producing `(t, +d)` and the next member consuming `(t, -d)` net to zero.
Since the internal edges are removed from the tracker, these intermediate changes are simply not reported.

### Internal capabilities

Each member reports internal capability changes through its `SharedProgress.internals`.
In the unfused case, the reachability tracker sees each member's capabilities at their respective source locations and computes implications through the graph.

The group scheduler aggregates each member's internal changes to the group outputs via a *capability map*.
This map is computed by a single reverse-topological pass over the group's internal DAG:

1. Seed: member output ports that are group outputs map directly to themselves.
2. Reverse pass: for each member from last to first in topological order, for each output port, follow internal edges forward to downstream members.
   Use the downstream member's summary to find which of its output ports are reachable from the targeted input port.
   Union the reachability sets.

This produces `capability_map[member][output_port] -> Vec<group_output_index>`.

When the group scheduler runs, it reads each member's `SharedProgress.internals` and reports them at every group output reached via the capability map.
Because all summaries are identity, this is equivalent to what the reachability tracker would compute by composing identity summaries along internal paths.

### Initial capability accounting

During `initialize()`, each member reports `+peers` capabilities at `T::minimum()` on its output ports.
The group transfers ALL members' initial capabilities to the group's `SharedProgress`, mapped through the capability map.
Members' initial internals are then cleared to prevent double-counting.

This is necessary because each member independently drops its initial capability during execution, producing `(-peers)` changes that flow through the capability map.
If only one member's `+peers` were reported, the tracker would go negative.

## Composed summary

The group's `internal_summary` describes which group outputs are reachable from which group inputs.
For each group input, the scheduler finds which member output ports are reachable (via the member's own summary), then follows the capability map to group outputs.
If a path exists, the summary entry is the identity; otherwise no entry exists.

This composed summary is used by the reachability tracker to determine implications from the group's sources to downstream operators.

## What does not fuse

Several classes of operators are excluded:

* **Frontier-observing operators** (`notify == true`): `inspect`, `unary_frontier`, and any operator that requests notifications.
  These need intra-group frontier propagation, which the group scheduler does not implement.
* **Operators with non-identity summaries**: the `Feedback` operator increments a loop counter coordinate.
  Fusing it would require the group to transform timestamps along internal paths.
* **Exchange-pact operators**: data moves between workers through channels outside the group scheduler's control.
* **Operators in iteration scopes**: the nested timestamp structure typically involves non-identity summaries at scope boundaries.

In practice, the operators that fuse are the "glue" operators: `map`, `flat_map`, `filter`, `Enter`, `Leave`, `Concatenate`, and similar single-purpose transformations.
In differential dataflow's BFS, fusion merges groups like `[Enter, Concatenate, Negate, AsCollection, Concatenate, ResultsIn]` into single scheduling units.

## Configuration

Fusion is controlled by `WorkerConfig::fuse_chain_length(n)`:

* `n >= 2` (default): fuse groups of at least `n` members.
* `n == 0` or `n == 1`: disable fusion entirely.

From the command line, pass `--fuse-chain-length N` to any timely program that uses `execute_from_args`.
