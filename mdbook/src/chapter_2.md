# Building Timely Dataflows

Let's talk about how to create timely dataflows.

This section will be a bit of a tour through the dataflow construction process, starting with getting data in to and out of a dataflow, through the main classes of operators you might like to use, as well as how to build your own operators.

Everything to do with dataflow construction happens within the timely worker, and we'll want to write it all inside the closure we supply to timely for each worker:

```rust,no_run
#![allow(unused_variables)]
extern crate timely;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        // define a new dataflow
        worker.dataflow::<(),_,_>(|scope| {

            // WE ARE GOING TO WRITE THIS STUFF!!!

        });

    }).unwrap();
}
```

Timely should keep us honest here, because we will need access to a dataflow scope (named `scope` in the example above) to create any dataflow operators. However, with that access we can start building up pretty exciting dataflows!

You may have notice that we had to supply some weird `<(),_,_>` decorator to the call to `dataflow`, and why is that, huh? The `dataflow` method is "generic", in that it works with many different types of .. "things". In this case, we need to specify three types to use `dataflow`: the timestamp type it should use, the type of closure we are asking it to invoke, and the type of data we plan to return. In most cases Rust can use type inference to infer these without the `<...>` nonsense, but since we aren't *doing* anything in the dataflow, we didn't give it enough hints. Here we are saying "use timestamp type `()`", and the `_` is special and means: "figure it out yourself" using type inference.

---

**NOTE**: Timely very much assumes that you are going to build the same dataflow on each worker. You don't literally have to, in that you could build a dataflow from user input, or with a random number generator, things like that. Please don't! It will not be a good use of your time.