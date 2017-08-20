# Flow Control

**IN PROGRESS**

Data flow along dataflow graphs. It is what they do. It says it in the name. But some times we want to control how the dataflow flow. Not *where* the data flow, we already have controls for doing that (`exchange`, `partition`), but rather *when* the data flow.

Let's consider a simple example, where we take an input stream of numbers, and produce all numbers less than each input.

```rust
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        // circulate numbers, decrementing each time.
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

Let's take a simple example, where we have a stream of timestamped numbers coming at us, performing the `flat_map` up above. Our goal is to 