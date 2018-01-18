# Flow Control

**IN PROGRESS**

Data flow along dataflow graphs. It is what they do. It says it in the name. But some times we want to control how the data in the dataflow flow. Not *where* the data flow, we already have controls for doing that (`exchange`, `partition`), but rather *when* the data flow.

Let's consider a simple example, where we take an input stream of numbers, and produce all numbers less than each input.

```rust
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        // Produce all numbers less than each input number.
        (1 .. 10)
            .to_stream(scope)
            .flat_map(|x| (0 .. x));

    });
}
```

Each number that you put into this dataflow (those `0 .. 10` folks) can produce a larger number of output records. Let's say you put `1000 .. 1010` as input instead? We'd have ten thousand records flowing around. What about `1000 .. 2000`?

This dataflow can greatly increase the amount of data moving around. We might have thought "let's process just 1000 records", but this turned into one million records. Perhaps we have a few of these operators in a row (don't ask; it happens), we can pretty quickly overwhelm the system if we aren't careful.

In most systems this is mitigated by *flow control*, mechanisms that push back when it seems like operators are producing more data than can be consumed in the same amount of time.

Timely dataflow doesn't have a built in notion of flow control. Some times you want it, some times you don't, so we didn't make you have it. Also, it is hard to get right, for similar reasons. Instead, timely dataflow scopes can be used for application-level flow control.

Let's take a simple example, where we have a stream of timestamped numbers coming at us, performing the `flat_map` up above. Our goal is to process all of the data, but to do so in a controlled manner where we never overwhelm the computation. For example, we might want to do approximately this:

```rust
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        // Produce all numbers less than each input number.
        (1 .. 100_000)
            .to_stream(scope)
            .flat_map(|x| (0 .. x));

    });
}
```

but without actually producing the 4,999,950,000 intermediate records all at once. 

One way to do this is to build a self-regulating dataflow, into which we can immediately dump all the records, but which will buffer records until it is certain that the work for prior records has drained. We will write this out in all the gorey detail, but these operators are certainly things that could be packaged up and re-used.

The idea here is to take our stream of work, and to use the `delay` operator to assign new timestamps to the records. We will spread the work out so that each timestamp has at most (in this case) 100 numbers. We can write a `unary` operator that will buffer received records until their timestamp is "next", meaning all strictly prior work has drained from the dataflow fragment. How do we do this? We put a `probe` just after the `flat_map`, and re-use the probe handle in the operator itself.

```rust
extern crate timely;

use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::ProbeHandle;

fn main() {
    timely::example(|scope| {

        let mut probe_handle1 = ProbeHandle::new();
        let probe_handle2 = probe_handle1.clone();

        let mut stash = ::std::collections::HashMap::new();

        // Produce all numbers less than each input number.
        (1 .. 100_000u64)
            .to_stream(scope)
            // Assign timestamps to records so that not much work is in each time.
            .delay(|number, time| {
                let mut time = time.clone();
                time.inner = number / 100;
                time
            })
            // Buffer records until all prior timestamps have completed.
            .unary(Pipeline, "Buffer", move |_capability| 
                move |input, output| {

                    // Stash received data.
                    input.for_each(|time, data| {
                        stash.entry(time)
                             .or_insert(Vec::new())
                             .extend(data.drain(..));
                    });

                    // Consider sending stashed data.
                    for (time, data) in stash.iter_mut() {
                        // Only send data once the probe is not less than the time.
                        // That is, once we have finished all strictly prior work.
                        if !probe_handle2.less_than(time.time()) {
                            output.session(&time).give_iterator(data.drain(..));
                        }
                    }

                    // discard used capabilities.
                    stash.retain(|_time, data| !data.is_empty());
                }
            )
            .flat_map(|x| (0 .. x))
            // Notice when work completes.
            .probe_with(&mut probe_handle1);
    });
}
```

This version of the code tops out at about 64MB on my laptop and takes 45 seconds, whereas the version with `unary` commented out heads north of 32GB before closing out after two minutes.
