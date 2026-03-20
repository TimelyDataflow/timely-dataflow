# Logging

Timely dataflow has a built-in logging infrastructure that lets you observe the internal behavior of a running computation. Every worker maintains a `Registry` of named logging streams. You can tap into these streams by registering closures that receive batches of events, and you can also create your own custom logging streams for application-level instrumentation.

## Tapping into logging

Each timely worker has a logging registry accessible via `worker.log_register()`. You register a logging callback by calling `insert` on the registry, providing a string name and a closure. The string name identifies which logging stream you want to listen to, and the closure is called with batches of events.

Here is a minimal example that prints all timely system events:

```rust,no_run
use timely::logging::TimelyEventBuilder;

timely::execute_from_args(std::env::args(), |worker| {

    // Register a callback for the "timely" logging stream.
    worker.log_register().unwrap()
        .insert::<TimelyEventBuilder, _>("timely", |time, data| {
            if let Some(data) = data {
                for event in data.iter() {
                    println!("{:?}", event);
                }
            }
        });

    worker.dataflow::<usize,_,_>(|scope| {
        // ... build your dataflow here ...
    });

}).unwrap();
```

The `insert` method is generic over two type parameters: a *container builder* type that describes how events are batched, and the closure type. Each built-in logging stream has a corresponding builder type alias (like `TimelyEventBuilder`) that you should use.

The closure signature is `FnMut(&Duration, &mut Option<Container>)`:

- The `&Duration` is the elapsed time since worker startup.
- `&mut Option<Container>` is `Some(container)` when delivering a batch of events, or `None` to signal a flush (e.g., when the worker is about to park or the stream is closing).

Each event in the container is a tuple `(Duration, Event)` where the `Duration` is the timestamp at which the event was logged.

## Retrieving loggers for custom events

You can also create your own logging streams. Register a stream with `insert`, then retrieve a `Logger` handle with `get` to log events from within your dataflow:

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

## Logging stream names

Timely uses string names to identify logging streams. The built-in stream names are:

| Stream name | Builder type | Event type | Description |
|---|---|---|---|
| `"timely"` | `TimelyEventBuilder` | `TimelyEvent` | Core system events: operator lifecycle, scheduling, messages, channels |
| `"timely/progress/{T}"` | `TimelyProgressEventBuilder<T>` | `TimelyProgressEvent<T>` | Progress protocol messages between operators |
| `"timely/summary/{T}"` | `TimelySummaryEventBuilder<TS>` | `OperatesSummaryEvent<TS>` | Operator connectivity summaries |
| `"timely/reachability/{T}"` | `TrackerEventBuilder<T>` | `TrackerEvent<T>` | Reachability tracker updates |

The `{T}` in the progress, summary, and reachability stream names is the Rust type name of the dataflow's timestamp, obtained from `std::any::type_name::<T>()` (e.g., `"timely/progress/usize"` for a dataflow using `usize` timestamps). This is because these events are generic over the timestamp type. Note that `type_name` is best-effort and not guaranteed to be stable across compiler versions, so these stream names should be treated accordingly.

You can register a callback for these at any point before or after building a dataflow. If you register a callback for a stream name that is already in use, the new callback takes effect for subsequently created loggers but existing loggers continue to use the old callback.

## The `TimelyEvent` variants

The `TimelyEvent` enum is the primary event type for the `"timely"` logging stream. Its variants capture the lifecycle and activity of a timely computation:

### `Operates(OperatesEvent)`

Logged when an operator is created. Contains the operator's worker-unique `id`, its hierarchical `addr` (the address of the scope containing the operator, matching the convention used by `ChannelsEvent`), and its `name`. The full address of the operator itself can be reconstructed as `addr ++ [id]`.

### `Channels(ChannelsEvent)`

Logged when a channel is created between operators. Contains the channel `id`, the `scope_addr` of the containing scope (not including the channel's own id), `source` and `target` descriptors (each an `(operator_index, port)` pair), and a `typ` string naming the data type carried on the channel.

### `Schedule(ScheduleEvent)`

Logged when an operator starts or stops executing. Contains the operator `id` and a `start_stop` field that is either `StartStop::Start` or `StartStop::Stop`. These events bracket each invocation of an operator's logic, and can be used to measure how much time each operator consumes.

### `Messages(MessagesEvent)`

Logged when a batch of messages is sent or received on a channel. Contains `is_send` (true for send, false for receive), the `channel` identifier, `source` and `target` worker indices, a `seq_no` sequence number, and the `record_count` of records in the batch.

### `PushProgress(PushProgressEvent)`

Logged when external progress updates are pushed onto an operator. Contains the `op_id` of the operator receiving the update.

### `Shutdown(ShutdownEvent)`

Logged when an operator is shut down. Contains the operator `id`.

### `Park(ParkEvent)`

Logged when a worker parks (goes idle waiting for work) or unparks. `ParkEvent::Park(Some(duration))` indicates a timed park, `ParkEvent::Park(None)` an indefinite park, and `ParkEvent::Unpark` the resumption of work.

### `CommChannels(CommChannelsEvent)`

Logged when a communication channel is established. Contains an `identifier` and a `kind` that is either `CommChannelKind::Progress` or `CommChannelKind::Data`.

### `Text(String)`

An unstructured text event for ad-hoc logging.

## Connecting operators and channels

The `OperatesEvent` and `ChannelsEvent` logs together describe the structure of a dataflow graph. To make sense of them, it helps to understand how addresses, operator indices, and ports relate to each other.

### From a channel to its operators

Both `OperatesEvent` and `ChannelsEvent` use the same convention: their address field (`addr` and `scope_addr`, respectively) is the address of the *containing scope*, not the entity itself. For an operator, the full address can be reconstructed as `addr ++ [id]`. For a channel, the `scope_addr` tells you which scope the channel lives in, and the `source`/`target` descriptors of the form `(operator_index, port)` identify operators within that scope.

To find the full address of a channel's source or target operator, concatenate the channel's `scope_addr` with the `operator_index`:

```text
operator address = scope_addr ++ [operator_index]
```

For example, if a channel has `scope_addr: [0]`, `source: (3, 0)`, and `target: (5, 1)`, then:
- The source operator has address `[0, 3]`, output port `0`.
- The target operator has address `[0, 5]`, input port `1`.

These operators will have `addr: [0]` in their `OperatesEvent` logs (the containing scope), with `id` values corresponding to the operator indices `3` and `5`.

### Child 0: the scope boundary

Every subgraph has a special "child 0" that represents the boundary between the subgraph and its surrounding scope. Child 0 is not a real operator; it is a stand-in for the scope that contains the subgraph. You will not see an `OperatesEvent` for child 0, but you will see channels that connect to it.

- **Child 0's outputs** correspond to data *entering* the subgraph. Output port `k` of child 0 is the `k`-th input of the subgraph operator as seen from the outer scope.

- **Child 0's inputs** correspond to data *leaving* the subgraph. Input port `k` of child 0 is the `k`-th output of the subgraph operator as seen from the outer scope.

So a channel with `target: (0, 2)` means "data leaves this subgraph and emerges at output port 2 of the operator that hosts this subgraph, in the outer scope." A channel with `source: (0, 1)` means "data arrives from input port 1 of the hosting operator, entering the subgraph."

This convention lets you trace data flow across scope boundaries: follow a channel to child 0, then find the corresponding port on the subgraph operator in the parent scope.

### Operator summaries: internal connectivity

While `ChannelsEvent` logs describe the *external* wiring between operators, an `OperatesSummaryEvent` describes an operator's *internal* topology: which of its inputs can result in data at which of its outputs, and what transformation is applied to timestamps along the way. This is the information reported by each operator during initialization, and it is what the progress tracking protocol uses to reason about which timestamps may still appear at downstream operators.

The `summary` field is a `Connectivity<TS>`, which is a `Vec<PortConnectivity<TS>>` indexed by input port. Each `PortConnectivity<TS>` maps output port indices to an `Antichain<TS>` of timestamp summaries. A summary `s` at position `(input_i, output_j)` means: "a record arriving at input `i` with timestamp `t` could produce a record at output `j` with timestamp `t.join(s)`." If an `(input, output)` pair has no entry, the operator guarantees that input can never cause output at that port.

For example, a `map` operator has one input and one output with an identity summary (timestamps pass through unchanged). An operator that delays output by one tick would have a summary that advances the timestamp. An operator with two inputs and one output (like `concat`) would report that both inputs connect to the single output with identity summaries.

This information is essential for understanding the progress guarantees of a dataflow. If you are trying to understand why a particular timestamp is not yet "complete" at some point in the graph, the operator summaries tell you which paths could still produce data at that timestamp.

## Other event types

### `TimelyProgressEvent<T>`

Events on the `"timely/progress/{T}"` streams capture the exchange of progress information between operators. Each event records whether it is a send or receive (`is_send`), the `source` worker, the `channel` and `seq_no`, the `identifier` of the operator, and two lists of updates: `messages` (updates to message counts at targets) and `internal` (updates to capabilities at sources). Each update is a tuple `(node, port, timestamp, delta)`.

These events are primarily useful for debugging the progress tracking protocol or understanding how capabilities flow through a dataflow.

### `TrackerEvent<T>`

Events on the `"timely/reachability/{T}"` streams record updates to the reachability tracker, which maintains the set of timestamps that could still arrive at each operator input. The variants are `SourceUpdate` and `TargetUpdate`, each carrying the node, port, timestamp, and delta of the update.

### `OperatesSummaryEvent<TS>`

Events on the `"timely/summary/{T}"` streams record the internal connectivity summary of each operator. See [Operator summaries: internal connectivity](#operator-summaries-internal-connectivity) above for a detailed explanation.

## Example: logging multiple streams

The following example registers callbacks for all four built-in logging streams and also creates a custom application-level logger:

```rust,no_run
use std::time::Duration;
use timely::logging::{
    TimelyEventBuilder, TimelyProgressEventBuilder, TimelySummaryEventBuilder,
};
use timely::container::CapacityContainerBuilder;
use timely::progress::reachability::logging::TrackerEventBuilder;

timely::execute_from_args(std::env::args(), |worker| {

    // Core timely events.
    worker.log_register().unwrap()
        .insert::<TimelyEventBuilder, _>("timely", |_time, data| {
            if let Some(data) = data {
                for event in data.iter() {
                    println!("TIMELY: {:?}", event);
                }
            }
        });

    // Progress tracking events (for usize timestamps).
    worker.log_register().unwrap()
        .insert::<TimelyProgressEventBuilder<usize>, _>(
            "timely/progress/usize", |_time, data| {
                if let Some(data) = data {
                    for event in data.iter() {
                        println!("PROGRESS: {:?}", event);
                    }
                }
            }
        );

    // Reachability tracker events.
    worker.log_register().unwrap()
        .insert::<TrackerEventBuilder<usize>, _>(
            "timely/reachability/usize", |_time, data| {
                if let Some(data) = data {
                    for event in data.iter() {
                        println!("REACHABILITY: {:?}", event);
                    }
                }
            }
        );

    // Operator summary events.
    worker.log_register().unwrap()
        .insert::<TimelySummaryEventBuilder<usize>, _>(
            "timely/summary/usize", |_time, data| {
                if let Some(data) = data {
                    for (_, event) in data.iter() {
                        println!("SUMMARY: {:?}", event);
                    }
                }
            }
        );

    // Custom application-level logger.
    type RoundBuilder = CapacityContainerBuilder<Vec<(Duration, usize)>>;
    worker.log_register().unwrap()
        .insert::<RoundBuilder, _>("my-app/rounds", |_time, data| {
            if let Some(data) = data {
                for (ts, round) in data.iter() {
                    println!("[{:?}] completed round {}", ts, round);
                }
            }
        });

    let round_logger = worker.log_register().unwrap()
        .get::<RoundBuilder>("my-app/rounds")
        .expect("Round logger absent");

    worker.dataflow::<usize,_,_>(|scope| {
        // ... build your dataflow ...
    });

    for round in 0..10 {
        // ... do work ...
        round_logger.log(round);
    }

}).unwrap();
```

## The `BatchLogger` adapter

If you want to feed logging events into a timely dataflow stream (for example, to analyze logs in real time or write them to durable storage), the `BatchLogger` struct bridges the two. It wraps an `EventPusher` and converts logging callbacks into a stream of timely `Event`s with progress information:

```rust,no_run
use timely::logging::BatchLogger;
```

`BatchLogger::publish_batch` is called with each `(&Duration, &mut Option<Container>)` from the logging closure, and it translates these into `Event::Messages` and `Event::Progress` updates suitable for consumption by `capture` and `replay` infrastructure.
