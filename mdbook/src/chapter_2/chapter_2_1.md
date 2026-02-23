# Creating Inputs

Let's start with the first thing we'll want for a dataflow computation: a source of data.

Almost all operators in timely can only be defined from a source of data, with a few exceptions. One of these exceptions is the `to_stream` operator, which is defined for various types and which takes a `scope` as an argument and produces a stream in that scope. Our `InputHandle` type from previous examples has a `to_stream` method, as well as any type that can be turned into an iterator (which we used in the preceding example).

For example, we can create a new dataflow with one interactive input and one static input:

```rust
extern crate timely;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::ToStream;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let mut input = InputHandle::new();

        // define a new dataflow
        worker.dataflow::<(),_,_>(|scope| {

            let stream1 = input.to_stream(scope).container::<Vec<String>>();
            let stream2 = (0 .. 10).to_stream(scope).container::<Vec<_>>();
        });

    }).unwrap();
}
```

There will be more to do to get data into `input`, and we aren't going to worry about that at the moment. But, now you know two of the places you can get data from!

At this point you may also have questions about the `container()` method with many symbols.
Rust is statically typed, and needs to know the type of things that each program will manipulate.
Timely dataflow bundles your individual data atoms into batches backed by a "container" type, and we need to communicate this type to Rust as well.
In our first case, we've said we have an input but we haven't provided any clues about the type of data, and must do so to compile the program even thought we don't care for the example.
In our second case, we've shown some data (integers) but we haven't revealed how we want to hold on to them, as we communicate the `Vec` structure and leave the data type unspecified with `_` (which Rust can fill in from the type of the integers).

## Other sources

There are other sources of input that are a bit more advanced. Once we learn how to create custom operators, the `source` method will allow us to create a custom operator with zero input streams and one output stream, which looks like a source of data (hence the name). There are also the `Capture` and `Replay` traits that allow us to exfiltrate the contents of a stream from one dataflow (using `capture_into`) and re-load it in another dataflow (using `replay_into`).
