# Timely Dataflow #

Timely dataflow is a low-latency cyclic dataflow computational model, introduced by [Naiad](http://research.microsoft.com/Naiad/). It's very neat, and if you are new to all of this I totally recommend the [Naiad paper](http://research.microsoft.com/pubs/201100/naiad_sosp2013.pdf).

This project is a flexible implementation of timely dataflow in [Rust](http://www.rust-lang.org). It's main feature is that it takes a new, much more modular approach to coordinating the timely dataflow computation. Naiad threw the entire dataflow graph in a big pile and, with enough restrictions and bits of tape, it all worked.

Our approach here is to organize things a bit more. While a dataflow graph may have vertices in it (where computation happens), these vertices can be backed by other timely dataflow graphs. There is relatively little information the parent scope needs to have about the child, and by maintaining that abstraction, we make several new things possible:

* subgraphs may use notions of progress other than ''iteration count'' as used in Naiad.
* subgraphs may coordinate among varying sets of workers, allowing tighter coordination when desired.
* subgraphs may be implementated in other languages and on other runtimes.
* subgraph progress is decoupled from the data plane, which may now be backed by other media and implementations.

There are other less-qualitative benefits, including improving performance due by removing the hash maps required (by Naiad) when the timely dataflow graph was not as structured. It is possible that there will be drawbacks to this design, though so far they have been restricted to having to think harder as part of designing the interface.

## Starting Out ##

After `git clone`-ing the repository, if you have [Rust](http://www.rust-lang.org) and [Cargo](https://crates.io) installed (Cargo comes with a Rust install), you should be able to type `cargo bench`. The examples currently assemble and "run" both a barrier micro-benchmark and a queueing micro-benchmark. The examples don't do anything useful!

On my laptop, eliding some whining about unused methods, it looks like this:
```
% cargo bench
Compiling timely v0.0.4 (file:///Users/mcsherry/Projects/timely-dataflow)
    Running target/release/timely-b7288f7ac38456ba

running 2 tests
test _barrier_bench ... bench:       220 ns/iter (+/- 64)
test _queue_bench   ... bench:      1203 ns/iter (+/- 269)

test result: ok. 0 passed; 0 failed; 0 ignored; 2 measured
```

You can also type `cargo build --release`, which will do a release build of `timely`. At this point, you can type `cargo run --release --bin timely`, and you should get usage information about further parameters, and modes to test out. You'll need the `--bin timely` because the project builds other executables, specifically one in `bin/command.rs` used to demonstrate hooking external processes as timely dataflow vertices.

## Caveats ##

This is a pet project, partly for learning a bit about Rust. While it is meant to be somewhat smarter and more flexible than Naiad as regards progress tracking, there are lots of things it doesn't yet do, and may never do. But, putting it out there in public may get other people thinking about whether and how they might help out, even if just by reading and commenting.

## Concepts ##

The project is presently a progress-tracking system, something like the dataflow equivalent of a scheduler. It manages the collective progress of various timely dataflow vertices and subgraphs, informing each participant as the system progresses to points where participants can be assured they will no longer receive messages bearing certain logical timestamps.

Two of the core concepts in timely dataflow are:

* `Timestamp`:  An element of a partially ordered set, attached to messages to indicate a logical time of sending.
                At any moment some number of messages are unprocessed, and their timestamps indicate unfinished work.

* `Summary`:    A function from `Timestamp` to `Timestamp`, describing the minimal progress a timestamp must make when traveling
                from one location in the timely dataflow graph to another. In control structures like loops, coordinates of
                the timestamps are explicitly advanced to distinguish different loop iterations.

From the set of outstanding timestamps and summaries of paths in the dataflow graph, one can reason about the possible future timestamps a location in the timely dataflow graph might receive. This allows us to deliver notifications of progress to timely dataflow elements who may await this information before acting.

## Scope Interface ##

We structure a timely dataflow graph as a collection of hierarchically nested `Scope`s, each of which has an associated `Timestamp` and `Summary` type, indicating the way in which its inputs and outputs understand progress. While scopes can be simple vertices, they may also contain other nested scopes, whose timestamps and their summaries can extend those of its parent.

The central features of the `Scope` interface involve methods for initialization, and methods for execution.

Initially, a scope must both describe its internal structure (so the parent can reason about messages moving through it) and learn about the external structure connecting its outputs back to its inputs (so that it can reason about how its messages might return to it). At runtime a scope must be able to respond to progress in the external graph (perhaps changes in which timestamps it may see in the future), and communicate any resulting progress it makes (including messages consumed from the external scope, produced for the external scope, and messages as yet unprocessed).

### Initialization ###

Before computation begins a `Scope` must indicate its structure to its parent `Scope`. This includes indicating the number of its inputs and outputs (so that others may connect to it), but also the internal connectivity between these inputs and outputs, as well as any initial internal capabilities to send messages. The internal connectivity is described by a collection of summaries for each input-output pair; we use a collection (technically, an `Antichain<Summary>`) rather than one summary because there may be several paths with incomparable summaries. The initial internal capabilities are explained by a map from `Timestamp` to a count for each output.

A `Scope` also receives information about the surrounding graph (which it can ignore, if it wishes). This information is roughly the dual of the information it supplies to its parent: for each output-input pair there is an `Antichain<Summary>` describing the possible paths from outputs to inputs, and for each input a map from `Timestamp` to a count, indicating initial message capabilities.

### Execution ###

Once initialized, a `Scope` interacts with its parent through a narrow interface. It receives information about the external changes to capabilities on each of its inputs, and it reports to its parent internal changes to the capabilities of its outputs, as well as the numbers of messages it has consumed (on each input) and produced (on each output). The fundamental safety property that a `Scope` must obey is to report any new capabilities no later than it reports consumed messages, and to report produced messages no later than it reports retired capabilities.

```rust
pub trait Scope<T: Timestamp> {
    fn inputs(&self) -> uint;   // number of inputs to the scope
    fn outputs(&self) -> uint;  // number of outputs from the scope

    // returns (input -> output) summaries and initial output message capabilities.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<CountMap<T>>);

    // receives (output -> input) summaries and initial input messages capabilities.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<S>>>,
                                       capabilities: &[CountMap<T>]) -> ();

    // receives changes in the message capabilities from the external graph.
    fn push_external_progress(&mut self, external: &mut [CountMap<T>]) -> ();

    // provides changes internal to the scope, specifically:
    //      * changes to messages capabilities for each output,
    //      * number of messages consumed on each input,
    //      * number of messages produced on each output.
    // return indicate unreported work still to do in the scope (e.g. IO, printing)
    fn pull_internal_progress(&mut self, internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool;
}
```

One non-obivous design (there are several) is that `pull_internal_progress` should indicate what messages were accepted by the scope, rather than have `push_external_progress` assign responsibility. We found the former worked better in Naiad, in that the scheduler did not need to understand the routing of messages; workers simply picked up what they were delivered, and told the scheduler, who eventually concludes that all messages are accounted for.

## A Data-parallel programming layer ##

The `Scope` interface is meant to be the bare-bones of timely dataflow, and it is important to present a higher level abstraction.

The project currently does this with a `Stream<Graph, Data>` type indicating a distributed stream of records of type `Data` living in some dataflow context indicated by `Graph`. By defining extension traits for the `Stream` type (new methods available to any instance of `Stream`) we can write programs in a more natural, declarative-ish style:

```rust
let mut stream = Input::open_source("path/to/data");

stream.filter(|x| x.len() > 5)
      .distinct()
      .inspect(|x| println!("observed: {}", x));
```

Each set of extension functions acts as a new "language" on the `Stream` types, except that they are fully composable, as the functions all render down to timely dataflow logic.
