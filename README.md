# Timely Dataflow #

Timely dataflow is a low-latency cyclic dataflow computational model, introduced in the paper [Naiad: a timely dataflow system](http://dl.acm.org/citation.cfm?id=2522738). This project is an extended and more modular implementation of timely dataflow in Rust.

This project is something akin to a distributed data-parallel compute engine, which scales the same program up from a single thread on your laptop to distributed execution across a cluster of computers. The main goals are expressive power and high performance. It is probably strictly more expressive and faster than whatever you are currently using, assuming you aren't yet using timely dataflow.

Be sure to read the [documentation for timely dataflow](https://docs.rs/timely). It is a work in progress, but mostly improving. There is more [long-form text](https://timelydataflow.github.io/timely-dataflow/) in `mdbook` format with examples tested against the current builds. There is also a series of blog posts ([part 1](https://github.com/frankmcsherry/blog/blob/master/posts/2015-09-14.md), [part 2](https://github.com/frankmcsherry/blog/blob/master/posts/2015-09-18.md), [part 3](https://github.com/frankmcsherry/blog/blob/master/posts/2015-09-21.md)) introducing timely dataflow in a different way, though be warned that the examples there may need tweaks to build against the current code.

# An example

To use timely dataflow, add the following to the dependencies section of your project's `Cargo.toml` file:

```
[dependencies]
timely="*"
```

This will bring in the [`timely` crate](https://crates.io/crates/timely) from [crates.io](http://crates.io), which should allow you to start writing timely dataflow programs like this one (also available in [timely/examples/simple.rs](https://github.com/timelydataflow/timely-dataflow/blob/master/timely/examples/simple.rs)):

```rust
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
               .inspect(|x| println!("seen: {:?}", x));
    });
}
```

You can run this example from the root directory of the `timely-dataflow` repository by typing

```
% cargo run --example simple
Running `target/debug/examples/simple`
seen: 0
seen: 1
seen: 2
seen: 3
seen: 4
seen: 5
seen: 6
seen: 7
seen: 8
seen: 9
```

This is a very simple example (it's in the name), which only just suggests at how you might write dataflow programs.

## Doing more things

For a more involved example, consider the very similar (but more explicit) [examples/hello.rs](https://github.com/timelydataflow/timely-dataflow/blob/master/examples/hello.rs), which creates and drives the dataflow separately:

```rust
extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("worker {}:\thello {}", index, x))
                 .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
```

This example does a fair bit more, to show off more of what timely can do for you.

We first build a dataflow graph creating an input stream (with `input_from`), whose output we `exchange` to drive records between workers (using the data itself to indicate which worker to route to). We `inspect` the data and print the worker index to indicate which worker received which data, and then `probe` the result so that each worker can see when all of a given round of data has been processed.

We then drive the computation by repeatedly introducing rounds of data, where the `round` itself is used as the data. In each round, each worker introduces the same data, and then repeatedly takes dataflow steps until the `probe` reveals that all workers have processed all work for that epoch, at which point the computation proceeds.

With two workers, the output looks like
```
% cargo run --example hello -- -w2
Running `target/debug/examples/hello -w2`
worker 0:   hello 0
worker 1:   hello 1
worker 0:   hello 2
worker 1:   hello 3
worker 0:   hello 4
worker 1:   hello 5
worker 0:   hello 6
worker 1:   hello 7
worker 0:   hello 8
worker 1:   hello 9
```

Note that despite worker zero introducing the data `(0..10)`, each element is routed to a specific worker, as we intended.

# Execution

The `hello.rs` program above will by default use a single worker thread. To use multiple threads in a process, use the `-w` or `--workers` options followed by the number of threads you would like to use. (note: the `simple.rs` program always uses one worker thread; it uses `timely::example` which ignores user-supplied input).

To use multiple processes, you will need to use the `-h` or `--hostfile` option to specify a text file whose lines are `hostname:port` entries corresponding to the locations you plan on spawning the processes. You will need to use the `-n` or `--processes` argument to indicate how many processes you will spawn (a prefix of the host file), and each process must use the `-p` or `--process` argument to indicate their index out of this number.

Said differently, you want a hostfile that looks like so,
```
% cat hostfile.txt
host0:port
host1:port
host2:port
host3:port
...
```
and then to launch the processes like so:
```
host0% cargo run -- -w 2 -h hostfile.txt -n 4 -p 0
host1% cargo run -- -w 2 -h hostfile.txt -n 4 -p 1
host2% cargo run -- -w 2 -h hostfile.txt -n 4 -p 2
host3% cargo run -- -w 2 -h hostfile.txt -n 4 -p 3
```
The number of workers should be the same for each process.

# The ecosystem

Timely dataflow is intended to support multiple levels of abstraction, from the lowest level manual dataflow assembly, to higher level "declarative" abstractions.

There are currently a few options for writing timely dataflow programs. Ideally this set will expand with time, as interested people write their own layers (or build on those of others).

* [**Timely dataflow**](https://docs.rs/timely/latest/timely/dataflow/operators/index.html): Timely dataflow includes several primitive operators, including standard operators like `map`, `filter`, and `concat`. It also includes more exotic operators for tasks like entering and exiting loops (`enter` and `leave`), as well as generic operators whose implementations can be supplied using closures (`unary` and `binary`).

* [**Differential dataflow**](https://github.com/timelydataflow/differential-dataflow): A higher-level language built on timely dataflow, differential dataflow includes operators like `group`, `join`, and `iterate`. Its implementation is fully incrementalized, and the details are pretty cool (if mysterious).

There are also a few applications built on timely dataflow, including [a streaming worst-case optimal join implementation](https://github.com/frankmcsherry/dataflow_join) and a [PageRank](https://github.com/frankmcsherry/pagerank) implementation, both of which should provide helpful examples of writing timely dataflow programs.

# Contributing

If you are interested in working with or helping out with timely dataflow, great!

There are a few classes of work that are helpful for us, and may be interesting for you. There are a few broad categories, and then an ever-shifting pile of issues of various complexity.

* If you would like to write programs using timely dataflow, this is very interesting for us. Ideally timely dataflow is meant to be an ergonomic approach to a non-trivial class of dataflow computations. As people use it and report back on their experiences, we learn about the classes of bugs they find, the ergonomic pain points, and other things we didn't even imagine ahead of time. Learning about timely dataflow, trying to use it, and reporting back is helpful!

* If you like writing little example programs or documentation tests, there are many places throughout timely dataflow where the examples are relatively sparse, or do not actually test the demonstrated functionality. These can often be easy to pick up, flesh out, and push without a large up-front obligation. It is probably also a great way to get one of us to explain something in detail to you, if that is what you are looking for.

* If you like the idea of getting your hands dirty in timely dataflow, the [issue tracker](https://github.com/timelydataflow/timely-dataflow/issues) has a variety of issues that touch on different levels of the stack. For example:

    * Timely currently [does more copies of data than it must](https://github.com/timelydataflow/timely-dataflow/issues/111), in the interest of appeasing Rust's ownership discipline most directly. Several of these copies could be elided with some more care in the resource management (for example, using shared regions of one `Vec<u8>` in the way that the [bytes crate](https://crates.io/crates/bytes) does). Not everything is obvious here, so there is the chance for a bit of design work too.

    * We recently landed a bunch of logging changes, but there is still [a list of nice to have features](https://github.com/timelydataflow/timely-dataflow/issues/114) that haven't made it yet. If you are interested in teasing out how timely works in part by poking around at the infrastructure that records what it does, this could be a good fit! It has the added benefit that the logs are timely streams themselves, so you can even do some log processing on timely. Whoa...

    * There is an open issue on [integrating Rust ownership idioms into timely dataflow](https://github.com/timelydataflow/timely-dataflow/issues/77). Right now, timely streams are of cloneable objects, and when a stream is re-used, items will be cloned. We could make that more explicit, and require calling a `.cloned()` method to get owned objects in the same way that iterators require it. At the same time, using a reference to a stream without taking ownership should get you the chance to look at the records that go past without taking ownership (and without requiring a clone, as is currently done). This is often plenty for exchange channels which may need to serialize the data and can't take much advantage of ownership anyhow.

    * There is a bunch of interesting work in scheduling timely dataflow operators, where when given the chance to schedule many operators, we might think for a moment and realize that several of them have no work to do and can be skipped. Better, we might maintain the list of operators with anything to do, and do nothing for those without work to do.

There are also some larger themes of work, whose solutions are not immediately obvious and each with the potential to sort out various performance issues:

## Rate-controlling output

At the moment, the implementations of `unary` and `binary` operators allow their closures to send un-bounded amounts of output. This can cause unwelcome resource exhaustion, and poor performance generally if the runtime needs to allocate lots of new memory to buffer data sent in bulk without being given a chance to digest it. It is commonly the case that when large amounts of data are produced, they are eventually reduced given the opportunity.

With the current interfaces there is not much to be done. One possible change would be to have the `input` and `notificator` objects ask for a closure from an input message or timestamp, respectively, to an output iterator. This gives the system the chance to play the iterator at the speed they feel is appropriate. As many operators produce data-parallel output (based on independent keys), it may not be that much of a burden to construct such iterators.

## Buffer management

The timely communication layer currently discards most buffers it moves through exchange channels, because it doesn't have a sane way of rate controlling the output, nor a sane way to determine how many buffers should be cached. If either of these problems were fixed, it would make sense to recycle the buffers to avoid random allocations, especially for small batches. These changes have something like a 10%-20% performance impact in the `dataflow-join` triangle computation workload.

## Support for non-serializable types

The communication layer is based on a type `Content<T>` which can be backed by typed or binary data. Consequently, it requires that the type it supports be serializable, because it needs to have logic for the case that the data is binary, even if this case is not used. It seems like the `Stream` type should be extendable to be parametric in the type of storage used for the data, so that we can express the fact that some types are not serializable and that this is ok.

**NOTE**: Differential dataflow demonstrates how to do this at the user level in its `operators/arrange.rs`, if somewhat sketchily (with a wrapper that lies about the properties of the type it transports).

This would allow us to safely pass Rc<T> types around, as long as we use the `Pipeline` parallelization contract.

## Coarse- vs fine-grained timestamps

The progress tracking machinery involves some non-trivial overhead per timestamp. This means that using very fine-grained timestamps, for example the nanosecond at which a record is processed, can swamp the progress tracking logic. By contrast, the logging infrastructure demotes nanoseconds to data, part of the logged payload, and approximates batches of events with the smallest timestamp in the batch. This is less accurate from a progress tracking point of view, but more performant. It may be possible to generalize this so that users can write programs without thinking about granularity of timestamp, and the system automatically coarsens when possible (essentially boxcar-ing times).

**NOTE**: Differential dataflow demonstrates how to do this at the user level in its `collection.rs`. The lack of system support means that the user ends up indicating the granularity, which isn't horrible but could plausibly be improved. It may also be that leaving the user with control of the granularity leaves them with more control over the latency/throughput trade-off, which could be a good thing for the system to do.
