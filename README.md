# Timely Dataflow #

Timely dataflow is a low-latency cyclic dataflow computational model, introduced by [Naiad](http://research.microsoft.com/Naiad/). This project is a flexible implementation of timely dataflow in [Rust](http://www.rust-lang.org), and whose main feature is that it identifies and uses a progress interface for timely dataflow subgraphs. This interface leads to several benefits:

* subgraphs may use notions of progress other than ''iteration count'' as used in Naiad.
* subgraphs may coordinate among varying sets of workers, allowing tighter coordination when desired.
* subgraphs may be implementated in other languages and on other runtimes.
* subgraph progress is decoupled from the data plane, which may now be backed by other media and implementations.

There are other less-qualitative benefits, including improving performance due by removing the hash maps required (by Naiad) when the timely dataflow graph was not as structured. It is possible that there will be drawbacks to this design, though so far they have been restricted to having to think harder as part of designing the interface.

## Starting Out ##

After `git clone`-ing the repository, if you have [Rust](http://www.rust-lang.org) and [Cargo](https://crates.io) installed (Cargo comes with a Rust install), you should be able to type any of `cargo build`, `cargo run`, and `cargo bench`. If you put `--release` after the first two, the performance will be a bit better. The examples currently assemble and "run" both a barrier micro-benchmark and a queueing micro-benchmark. The examples don't do anything useful!

On my laptop, eliding some whining about unused methods, it looks like this:
```
% cargo bench
Compiling timely v0.0.1 (file:///Users/mcsherry/Projects/timely-dataflow)
    Running target/release/timely-d1e180029f621076

    running 2 tests
    test test_barrier_bench ... bench:       185 ns/iter (+/- 68)
    test test_queue_bench   ... bench:      2178 ns/iter (+/- 764)

    test result: ok. 0 passed; 0 failed; 0 ignored; 2 measured
```

## Caveats ##

This is a pet project, partly for learning a bit about Rust. While it is meant to be somewhat smarter and more flexible than Naiad as regards progress tracking, there are lots of things it doesn't yet do, and may never do. But, putting it out there in public may get other people thinking about whether and how they might help out, even if just by reading and commenting.

## Concepts ##

The project is presently a progress-tracking system, something like the dataflow equivalent of a scheduler. It manages the collective progress of various timely dataflow vertices and subgraphs, informing each participant as the system progresses to points where participants can be assured they will no longer receive messages bearing certain logical timestamps.

Two of the core concepts in timely dataflow are:

* `Timestamp`:  An element of a partially ordered set, attached to messages to indicate a logical time of sending.
                At any moment some number of messages are unprocessed, and their timestamps indicate unfinished work.

* `PathSummary`:    Function from `Timestamp` to `Timestamp`, describing the minimal progress a timestamp must make when traveling
                    from one location in the timely dataflow graph to another. In control structures like loops, coordinates of
                    the timestamps are explicitly advanced to distinguish different loop iterations.

From the set of outstanding timestamps and path summaries of the graph, one can reason about the possible future timestamps a location in the timely dataflow graph might receive. This allows us to deliver notifications of progress to timely dataflow elements who may await this information before acting.

## Scope Interface ##

We structure a timely dataflow graph as a collection of hierarchically nested `Scope`s, each of which has an associated `Timestamp` and `PathSummary` type, indicating the way in which its inputs and outputs understand progress. While scopes can be simple vertices, they may also contain other Scopes, whose timestamps and path summaries can extend those of its parent.

Initially, a scope must both describe its internal structure (so the parent can reason messages moving through it) and learn about the external structure connecting its outputs back to its inputs (so that it can reason about how its messages might return to it). At runtime a scope must be able to respond to progress in the external graph (perhaps changes in which timestamps it may see in the future), and communicate any resulting progress it makes (including messages from the external scope, produced for the external scope, and as yet unprocessed).

1. Initialization:  a. describing its internal path summary structure (inputs -> outputs) and initial message capabilities,
                    b. learning about the external path summary structure (outputs -> inputs) and initial message capabilities.

2. Execution:       a. receiving updates about progress in the external dataflow graph,
                    b. communicating updates about progress within the scope to the containing scope.

To complete the signature of the `Scope` trait, a scope must also present a number of inputs and outputs (as progress will be specific to the inputs and outputs of the scope, and must be separately indicated). To understanding the trait, the type `Antichain<T: PartialOrd>` indicates a set of partially ordered elements none of which are strictly less than any other.

```rust
pub trait Scope<T: Timestamp, S: PathSummary<T>>
{
    fn inputs(&self) -> uint;   // number of inputs to the scope
    fn outputs(&self) -> uint;  // number of outputs from the scope

    // 1a. returns (input -> output) summaries, and initial message capabilities on outputs.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>);

    // 1b. receives (output -> input) summaries, and initial messages capabilities on inputs.
    fn set_external_summary(&mut self, summaries: Vec<Vec<Antichain<S>>>,
                                       frontier: &Vec<Vec<(T, i64)>>) -> ();

    // 2a. receives changes in the message capabilities from the external graph.
    fn push_external_progress(&mut self, frontier_progress: &Vec<Vec<(T, i64)>>) -> ();

    // 2b. describing changes internal to the scope, specifically:
    //      * changes to messages capabilities for each output,
    //      * number of messages consumed on each input,
    //      * number of messages produced on each output.
    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                         messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                         messages_produced: &mut Vec<Vec<(T, i64)>>) -> ();
}
```

One non-obivous design (there are several) is that `pull_internal_progress` should be responsible for indicating what messages were accepted by the scope, rather than have `push_external_progress` assign responsibility. We found the former worked better in Naiad, in that the scheduler did not need to understand the routing of messages; workers simply picked up what they were delivered, and told the scheduler, who eventually concludes that all messages are accounted for.

## Scope Implementation ##

A scope must implement the trait above, but is otherwise unconstrained. Notice that the trait does not detail how data are moved around, only how a scope should acknowledge receipt of data, production of data, and any outstanding work. A concrete scope implementation will almost certainly (one hopes) describe how to process some data, including mechanisms for receiving, acting on, and transmiting this data. However, this is not part of the scope interface.

The `Subgraph` struct implements the `Scope` trait, with a generic implementation as a container for other scopes.

The `src/example/` subdirectory has some example classes, likely to be cleaned up and included officially, implementing corresponding scopes and data movement for simple timely dataflow vertices, including `Input`, `Concat`, `Feedback`, and `Queue`. They are probably more complicated than they need to be, but as common patterns are extracted, custom vertices should become simpler.
