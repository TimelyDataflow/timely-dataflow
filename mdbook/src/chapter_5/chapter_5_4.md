# Logging

Timely dataflow provides a comprehensive logging infrastructure that records structural and runtime events as the dataflow executes.
These events allow you to reconstruct the dataflow graph, understand how data flows across scope boundaries, and profile operator execution.

All events are logged to named log streams, and each event carries a `Duration` timestamp (elapsed time since the worker started).
The primary log stream is `"timely"`, which carries `TimelyEvent` variants.
Additional typed log streams exist for progress, summary, and reachability information.

## Structural Events

These events describe the shape of the dataflow graph. They are logged once during construction.

### OperatesEvent

Logged when an operator is created within a scope.

| Field  | Type         | Description |
|--------|--------------|-------------|
| `id`   | `usize`      | Worker-unique identifier for the operator, allocated by the worker. |
| `addr` | `Vec<usize>` | Hierarchical address: the path from the root scope to this operator. |
| `name` | `String`     | Human-readable name (e.g. `"Map"`, `"Feedback"`, `"Subgraph"`). |

The `addr` field encodes the nesting structure.
For example, an address of `[0, 2, 1]` means: child 0 of the root, then child 2 within that scope, then child 1 within that.
Within any scope, child indices start at 1 for actual operators; index 0 is reserved (see [Scope Boundary Conventions](#scope-boundary-conventions) below).

The `id` field is a flat, worker-unique integer.
It is the key used by all other events (`ScheduleEvent`, `ShutdownEvent`, `MessagesEvent` via channels, etc.) to refer to this operator.
Two different workers will generally assign different `id` values to corresponding operators, but the `addr` will be the same.

### ChannelsEvent

Logged when a data channel is created between two operators (or between an operator and a scope boundary).

| Field        | Type             | Description |
|--------------|------------------|-------------|
| `id`         | `usize`          | Worker-unique channel identifier. |
| `scope_addr` | `Vec<usize>`     | Address of the scope that *contains* this channel. |
| `source`     | `(usize, usize)` | `(operator_index, output_port)` of the source within the containing scope. |
| `target`     | `(usize, usize)` | `(operator_index, input_port)` of the target within the containing scope. |
| `typ`        | `String`         | The container type transported on this channel, as a string. |

The `source` and `target` tuples use **scope-local** operator indices (not the worker-unique `id` from `OperatesEvent`).
To resolve them, find the `OperatesEvent` whose `addr` equals `scope_addr` with the operator index appended.
For example, if `scope_addr` is `[0, 2]` and `source` is `(3, 0)`, the source operator has address `[0, 2, 3]` and you want output port 0.

When either the source or target operator index is 0, the channel crosses a scope boundary. See [Scope Boundary Conventions](#scope-boundary-conventions).

### CommChannelsEvent

Logged when a communication channel (for inter-worker exchange) is established.

| Field        | Type              | Description |
|--------------|-------------------|-------------|
| `identifier` | `usize`           | Communication channel identifier. |
| `kind`       | `CommChannelKind` | Either `Progress` or `Data`. |

## Runtime Events

These events describe what happens as the dataflow executes.

### ScheduleEvent

Logged when an operator begins or finishes a scheduling invocation.

| Field        | Type        | Description |
|--------------|-------------|-------------|
| `id`         | `usize`     | Worker-unique operator identifier (same as `OperatesEvent::id`). |
| `start_stop` | `StartStop` | `Start` when the operator begins executing, `Stop` when it returns. |

A matched pair of `Start` and `Stop` events brackets one invocation of the operator's `schedule()` method.
These pairs let you measure per-operator execution time.

### MessagesEvent

Logged when a batch of data is sent or received on a channel.

| Field          | Type    | Description |
|----------------|---------|-------------|
| `is_send`      | `bool`  | `true` for a send, `false` for a receive. |
| `channel`      | `usize` | Channel identifier (same as `ChannelsEvent::id`). |
| `source`       | `usize` | Source worker index. |
| `target`       | `usize` | Target worker index. |
| `seq_no`       | `usize` | Sequence number for this (source, target) pair on this channel. |
| `record_count` | `i64`   | Number of records in the batch. |

For channels that stay within a single worker, `source` and `target` will be the same worker index.
For exchange (inter-worker) channels, they may differ.
The `record_count` comes from the container's `Accountable` trait implementation (e.g. `Vec::len()` cast to `i64`).

### ShutdownEvent

Logged when an operator is permanently shut down.

| Field | Type    | Description |
|-------|---------|-------------|
| `id`  | `usize` | Worker-unique operator identifier. |

### PushProgressEvent

Logged when frontier changes are pushed to an operator.

| Field   | Type    | Description |
|---------|---------|-------------|
| `op_id` | `usize` | Worker-unique operator identifier. |

### ParkEvent

Logged when a worker parks (goes idle waiting for external events) or wakes up.

| Variant            | Description |
|--------------------|-------------|
| `Park(Option<Duration>)` | Worker parks, with an optional maximum sleep duration. |
| `Unpark`           | Worker wakes from a parked state. |

### Text(String)

An unstructured text event for ad-hoc logging.

## Scope Boundary Conventions

Understanding scope boundaries is essential for interpreting `ChannelsEvent` data and reconstructing the full dataflow graph across nested scopes.

### Child Zero

By convention, **child index 0** within any scope is a pseudo-operator representing the scope's own boundary — its interface with its parent.
It is not a real operator; you will not see an `OperatesEvent` for child zero.
Instead, child zero is the mechanism by which channels inside a scope connect to channels outside.

Child zero's ports are **inverted** relative to the scope's external interface:

- **Child zero's outputs** are the scope's **inputs** (data arriving from the parent).
- **Child zero's inputs** are the scope's **outputs** (data leaving to the parent).

This inversion makes the internal wiring uniform: every channel inside a scope connects an operator output to an operator input, even when one end is the scope boundary.

### Connecting Parent and Child

When a scope (say, an iterative scope) appears as operator `K` in its parent, and you look inside that scope, the relationship is:

| Parent perspective | Child perspective |
|--------------------|-------------------|
| Operator `K`, input port `i` | Child zero, output port `i` |
| Operator `K`, output port `j` | Child zero, input port `j` |

So if you see a `ChannelsEvent` in the parent scope with `target: (K, i)`, the data enters the child scope and appears as if it came from child zero's output port `i`.
Inside the child scope, a `ChannelsEvent` with `source: (0, i)` connects that incoming data to whatever internal operator consumes it.

Similarly, data produced inside the child scope that should leave the scope is connected via a `ChannelsEvent` with `target: (0, j)` inside the child scope, and emerges as output port `j` of operator `K` in the parent scope.

### Worked Example

Consider a dataflow with an iterative scope:

```text
worker.dataflow(|scope| {                      // root scope, addr [0]
    input                                       // operator at [0, 1]
        .enter(scope.iterative(|inner| {        // iterative scope at [0, 2]
            inner
                .map(...)                        // operator at [0, 2, 1]
                .filter(...)                     // operator at [0, 2, 2]
        }))
        .inspect(...)                           // operator at [0, 3]
});
```

You would see structural events like:

1. `OperatesEvent { id: _, addr: [0],       name: "Dataflow" }` — the root scope itself.
2. `OperatesEvent { id: _, addr: [0, 1],    name: "Input" }` — the input operator.
3. `OperatesEvent { id: _, addr: [0, 2],    name: "Iterative" }` — the iterative scope (appears as an operator in the root).
4. `OperatesEvent { id: _, addr: [0, 2, 1], name: "Map" }` — the map, inside the iterative scope.
5. `OperatesEvent { id: _, addr: [0, 2, 2], name: "Filter" }` — the filter, inside the iterative scope.
6. `OperatesEvent { id: _, addr: [0, 3],    name: "Inspect" }` — the inspect, in the root scope.

Channel events in the root scope (`scope_addr: [0]`) connecting `Input` to the iterative scope:
- `ChannelsEvent { scope_addr: [0], source: (1, 0), target: (2, 0), ... }` — from Input (index 1) output 0 to the iterative scope (index 2) input 0.

Channel events inside the iterative scope (`scope_addr: [0, 2]`):
- `ChannelsEvent { scope_addr: [0, 2], source: (0, 0), target: (1, 0), ... }` — from child zero's output 0 (= scope input 0) to Map's input 0.
- `ChannelsEvent { scope_addr: [0, 2], source: (1, 0), target: (2, 0), ... }` — from Map's output 0 to Filter's input 0.
- `ChannelsEvent { scope_addr: [0, 2], source: (2, 0), target: (0, 0), ... }` — from Filter's output 0 to child zero's input 0 (= scope output 0).

And back in the root scope:
- `ChannelsEvent { scope_addr: [0], source: (2, 0), target: (3, 0), ... }` — from the iterative scope's output 0 to Inspect's input 0.

This chain shows data flowing: Input → [into scope via child zero] → Map → Filter → [out of scope via child zero] → Inspect.

### Reconstructing the Full Graph

To reconstruct the dataflow graph from logged events:

1. **Build the operator tree** from `OperatesEvent` entries, using `addr` to establish parent-child relationships. Any operator whose `addr` has length `n` is a child of the operator (scope) whose `addr` is the first `n-1` elements.

2. **Build per-scope channel graphs** from `ChannelsEvent` entries. Group channels by `scope_addr`. Within each scope, the `source` and `target` pairs give you directed edges between scope-local operator indices.

3. **Stitch across scope boundaries** using child zero. When a channel in scope `S` has source or target operator index 0, it connects to the scope's external interface. Find the operator in `S`'s parent that represents this scope, and link the corresponding port.

4. **Correlate runtime events** using the worker-unique `id` from `OperatesEvent` to join `ScheduleEvent`, `ShutdownEvent`, and other events. Use `ChannelsEvent::id` to join `MessagesEvent` records to their channel.

### Operator Summaries: Internal Connectivity

While `ChannelsEvent` logs describe the *external* wiring between operators, an `OperatesSummaryEvent` describes an operator's *internal* topology: which of its inputs can result in data at which of its outputs, and what transformation is applied to timestamps along the way. This is the information reported by each operator during initialization, and it is what the progress tracking protocol uses to reason about which timestamps may still appear at downstream operators.

The `summary` field is a `Connectivity<TS>`, which is a `Vec<PortConnectivity<TS>>` indexed by input port. Each `PortConnectivity<TS>` maps output port indices to an `Antichain<TS>` of timestamp summaries. A summary `s` at position `(input_i, output_j)` means: "a record arriving at input `i` with timestamp `t` could produce a record at output `j` with timestamp `t.join(s)`." If an `(input, output)` pair has no entry, the operator guarantees that input can never cause output at that port.

For example, a `map` operator has one input and one output with an identity summary (timestamps pass through unchanged). An operator that delays output by one tick would have a summary that advances the timestamp. An operator with two inputs and one output (like `concat`) would report that both inputs connect to the single output with identity summaries.

This information is essential for understanding the progress guarantees of a dataflow. If you are trying to understand why a particular timestamp is not yet "complete" at some point in the graph, the operator summaries tell you which paths could still produce data at that timestamp.

## Additional Log Streams

Beyond the main `"timely"` stream, there are typed log streams for deeper introspection:

| Stream name | Builder type | Event type | Description |
|---|---|---|---|
| `"timely"` | `TimelyEventBuilder` | `TimelyEvent` | Core system events: operator lifecycle, scheduling, messages, channels |
| `"timely/progress/<T>"` | `TimelyProgressEventBuilder<T>` | `TimelyProgressEvent<T>` | Progress protocol messages between operators |
| `"timely/summary/<T>"` | `TimelySummaryEventBuilder<TS>` | `OperatesSummaryEvent<TS>` | Operator connectivity summaries (see above) |
| `"timely/reachability/<T>"` | `TrackerEventBuilder<T>` | `TrackerEvent<T>` | Reachability tracker updates |

The `<T>` in the stream names is the Rust type name of the dataflow's timestamp, obtained from `std::any::type_name::<T>()` (e.g., `"timely/progress/usize"` for a dataflow using `usize` timestamps). Note that `type_name` is best-effort and not guaranteed to be stable across compiler versions, so these stream names should be treated accordingly.

Because the typed-stream names embed `type_name::<T>()`, the exact key for a given dataflow can be awkward to predict (especially for nested or composite timestamps). `Registry::names()` returns an iterator over the names currently bound, which you can call after constructing a dataflow to see what is available:

```rust,no_run
timely::execute_from_args(std::env::args(), |worker| {
    worker.dataflow::<usize,_,_>(|scope| {
        // ... build your dataflow ...
    });

    if let Some(registry) = worker.log_register() {
        for name in registry.names() {
            println!("{name}");
        }
    }
}).unwrap();
```

**`TimelyProgressEvent<T>`** captures the exchange of progress information between operators. Each event records whether it is a send or receive (`is_send`), the `source` worker, the `channel` and `seq_no`, the `identifier` of the operator, and two lists of updates: `messages` (updates to message counts at targets) and `internal` (updates to capabilities at sources). Each update is a tuple `(node, port, timestamp, delta)`. These are primarily useful for debugging the progress tracking protocol.

**`TrackerEvent<T>`** records updates to the reachability tracker, which maintains the set of timestamps that could still arrive at each operator port. Each scope (subgraph) has its own tracker, identified by `tracker_id` — this is the worker-unique `id` of the scope operator (the same `id` from `OperatesEvent`).

The tracker monitors two kinds of locations:

- **Targets** (operator input ports): timestamps of messages that may still arrive.
- **Sources** (operator output ports): timestamps of capabilities that operators still hold.

The `TrackerEvent` enum has two variants:

| Variant | Fields | Description |
|---------|--------|-------------|
| `SourceUpdate` | `tracker_id`, `updates` | Changes to capability counts at operator output ports. |
| `TargetUpdate` | `tracker_id`, `updates` | Changes to message counts at operator input ports. |

Each entry in `updates` is a tuple `(node, port, timestamp, delta)`:

| Field | Type | Description |
|-------|------|-------------|
| `node` | `usize` | Scope-local operator index (same convention as `ChannelsEvent` source/target indices, including 0 for the scope boundary). |
| `port` | `usize` | Port index on that operator. |
| `timestamp` | `T` | The timestamp being updated. |
| `delta` | `i64` | The change in count: positive means a new capability or pending message; negative means one was retired. |

A `SourceUpdate` with positive `delta` means an operator has acquired (or retained) a capability to produce data at that timestamp on that output port. A negative `delta` means it has released one. Similarly, a `TargetUpdate` with positive `delta` means messages at that timestamp may still arrive at that input port; negative means some have been accounted for.

The frontier at any location is the set of timestamps with positive accumulated count. When all counts at a target reach zero for a given timestamp, the operator knows no more messages at that timestamp will arrive — this is the mechanism by which operators learn they can "close" a timestamp and make progress.

### Using Reachability Logging for Debugging

The `TrackerEvent` stream is particularly useful for diagnosing progress-tracking issues — for example, understanding why a dataflow appears stuck or why a particular timestamp hasn't completed.

**Reconstructing capability state.** Since each event carries a `delta`, you can reconstruct the current capability state at any point by accumulating deltas. For each `(tracker_id, node, port, timestamp)`, sum the deltas from all `SourceUpdate` events. A positive sum means the operator currently holds a capability at that timestamp on that port. When the sum reaches zero, the capability has been fully released.

The same applies to `TargetUpdate` events for message counts: a positive accumulated count at a target means messages at that timestamp may still be in flight.

**Identifying a stuck dataflow.** When a dataflow hangs, the accumulated state tells you exactly which operators hold capabilities and at which timestamps. Cross-reference the `tracker_id` with `OperatesEvent` to identify the scope, and the `node` with operator addresses within that scope (recall that `node` is a scope-local index, and the operator's full address is the scope's `addr` with `node` appended).

For example, if accumulated `SourceUpdate` deltas show that node 5 in tracker 7 holds a capability at timestamp `(42, 3)`, and `OperatesEvent` tells you tracker 7 is the scope at address `[0, 2]`, then the operator at address `[0, 2, 5]` holds the capability. Look up its name in the `OperatesEvent` log to identify it.

**Understanding why a frontier hasn't advanced.** The frontier at an operator's input can only advance when all upstream capabilities that could produce data at the current frontier timestamps have been released. The `SourceUpdate` events let you identify which operators still hold such capabilities. Trace the graph (using `ChannelsEvent` and operator summaries from `OperatesSummaryEvent`) from those capabilities forward to the stuck operator's input to understand the dependency chain.

**Matching scopes to log streams.** Each scope has its own tracker and its own log stream. A dataflow using `usize` timestamps with a nested iterative scope would produce two reachability streams: `"timely/reachability/usize"` for the root scope, and something like `"timely/reachability/Product<usize, u64>"` for the iterative scope (where the `u64` is the iteration counter). Register loggers for each stream you want to observe.

## Registering a Logger

To consume logging events, register a callback with the worker's log registry. The `insert` method takes a string name and a closure:

```rust,no_run
use timely::logging::TimelyEventBuilder;

timely::execute_from_args(std::env::args(), |worker| {

    worker.log_register().unwrap()
        .insert::<TimelyEventBuilder, _>("timely", |time, data| {
            if let Some(data) = data {
                for (elapsed, event) in data.iter() {
                    println!("{elapsed:?}\t{event:?}");
                }
            }
        });

    worker.dataflow::<usize,_,_>(|scope| {
        // ... build your dataflow ...
    });

}).unwrap();
```

The `insert` method is generic over two type parameters: a *container builder* type that describes how events are batched, and the closure type. Each built-in logging stream has a corresponding builder type alias (like `TimelyEventBuilder`) that you should use.

The closure signature is `FnMut(&Duration, &mut Option<Container>)`:

- The `&Duration` is the elapsed time since worker startup.
- `&mut Option<Container>` is `Some(container)` when delivering a batch of events, or `None` to signal a flush (e.g., when the worker is about to park or the stream is closing).

Each event in the container is a tuple `(Duration, Event)` where the `Duration` is the timestamp at which the event was logged.

You can register a callback for a stream at any point before or after building a dataflow. If you register a callback for a stream name that is already in use, the new callback takes effect for subsequently created loggers but existing loggers continue to use the old callback.

## Custom Logging Streams

You can create your own logging streams. Register a stream with `insert`, then retrieve a `Logger` handle with `get` to log events from your application code:

```rust,no_run
use std::time::Duration;
use timely::container::CapacityContainerBuilder;

timely::execute_from_args(std::env::args(), |worker| {

    // Define a container builder for your event type.
    type MyBuilder = CapacityContainerBuilder<Vec<(Duration, String)>>;

    // Register the logging stream with a callback.
    worker.log_register().unwrap()
        .insert::<MyBuilder, _>("my-app/events", |_time, data| {
            if let Some(data) = data {
                for (ts, msg) in data.iter() {
                    println!("[{:?}] {}", ts, msg);
                }
            }
        });

    // Retrieve a Logger handle to use later.
    let my_logger = worker.log_register().unwrap()
        .get::<MyBuilder>("my-app/events")
        .expect("Logger was just registered");

    worker.dataflow::<usize,_,_>(|scope| {
        // ... build your dataflow ...
    });

    // Log events from your application logic.
    my_logger.log("something happened".to_string());

}).unwrap();
```

The `Logger` buffers events internally and flushes them to the registered closure when the buffer reaches capacity, when `flush()` is called explicitly, or when the logger is dropped.

You can also use `BatchLogger` to forward events into a timely capture stream for downstream processing via the `capture` and `replay` infrastructure.

## Communication Thread Logging

The logging described above all runs on worker threads and is accessed through the worker's `Registry`. In multi-process (cluster) deployments, timely also runs dedicated send and receive threads for TCP networking. These threads have their own logging, configured separately.

Communication logging is configured via the `log_fn` field in `Config::Cluster`. This is a closure that receives a `CommunicationSetup` describing the thread (whether it is a sender or receiver, and which processes it connects) and returns an `Option<Logger<CommunicationEventBuilder>>`. By default, this closure returns `None` and no communication events are logged.

The `CommunicationEvent` enum has three variants:

| Variant | Description |
|---------|-------------|
| `Setup(CommunicationSetup)` | Identifies the thread: whether it is a `sender` or receiver, the local `process` id, and the `remote` process id. |
| `State(StateEvent)` | Thread start or stop. Contains `send` (is this a send thread), `process` and `remote` ids, and `start` (true when starting, false when stopping). |
| `Message(MessageEvent)` | Message send or receive. Contains `is_send` and the message `header` (which includes channel, source, target, and length). |

These events are only relevant when running across multiple processes. For single-process or single-thread configurations, no communication threads are created and these events will not appear.
