# Creating Inputs

Let's start with the first thing we'll want for a dataflow computation: a source of data.

Almost all operators in timely can only be defined from a source of data, with a few exceptions. One of those exceptions is the input operator. How do we get such an operator? As it turns out, there are some handy methods that dataflow scopes provide for us. One of these is `new_input()`. 

```rust,no_run
#![allow(unused_variables)]
#![allow(unused_variables)]
extern crate timely;

use timely::dataflow::operators::Input;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        // define a new dataflow
        worker.dataflow::<(),_,_>(|scope| {

            // WE ARE GOING TO WRITE THIS STUFF!!!
            let (handle, stream) = scope.new_input::<String>();

        });

    }).unwrap();
}
```

The call to `new_input()` returns two things: a `handle` that we the Rust program can use to supply data to the dataflow, and a `stream` of that data once it has been introduced into the dataflow. We are going to want to return `handle` upward so that our worker can actually use it once we've finished building the dataflow, and we'll probably want to use `stream` as the basis of further computation.

The `dataflow()` method we invoked can return things. Anything our closure returns, the method will return. This is handy if we want to extract the handle so that we can use it after the dataflow is constructed. We might reasonably write:

```rust,no_run
#![allow(unused_variables)]
#![allow(unused_mut)]
extern crate timely;

use timely::dataflow::operators::Input;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        // define a new dataflow
        let mut handle = worker.dataflow::<(),_,_>(|scope| {
            let (handle, stream) = scope.new_input::<String>();
            handle
        });

    }).unwrap();
}
```

In Rust you return things just by having them be the last expression in a sequence, and *not* putting a semicolon after them. Or rather, if you put a semicolon after the last item you return the result, which happens to be the type `()`.

Up about we've bound the handle to another variable named `handle` using `let mut handle = ...`. This is how we explain to Rust that we are taking ownership of the returned handle, and moreover that we might plan on doing some mutation with this handle (if we want to send some data at it, for example).

There are other ways of constructing inputs, that vary according to taste. Another way you can accomplish what we've just done above is to first create `handle` outside of the dataflow, unattached to any stream, and then use the `input_from` method to bind it to a stream:

```rust,no_run
#![allow(unused_variables)]
#![allow(unused_variables)]
extern crate timely;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::Input;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        // define `handle` before the dataflow.
        let mut handle = InputHandle::<(), String>::new();

        // define a new dataflow
        worker.dataflow(|scope| {
            // attach `handle` in the dataflow.
            scope.input_from(&mut handle);
        });

    }).unwrap();
}
```

This approach is clearer for some, but could also be error-prone: you might create a handle and forget to attach it. I suppose at the same time you could create an input stream and forget to use it (like we did above), so this distinction is perhaps to taste.

Also notice that while we do have to specific `<(), String>` to create the new input handle (timestamp and data types), we no longer need to do so for the `dataflow` method. Independently, you might wonder why the `<...>` stuff goes before the method here; it's just a matter that the *type* `InputHandle` is generic and we are specifying its parameters (not the `new` method's), whereas the *method* `dataflow` is generic and we were specifying its parameters (not the scope's).

## Non-interactive inputs

You can also create non-interactive inputs, if you'd like part of your computation to run even without your help. The `to_stream` method is defined on any iterator, and introduces all of the data the iterator produces into a dataflow stream.

```rust,no_run
#![allow(unused_variables)]
#![allow(unused_variables)]
extern crate timely;

use timely::dataflow::operators::ToStream;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        // define a new dataflow
        worker.dataflow::<(),_,_>(|scope| {
            // attach `handle` in the dataflow.
            (0 .. 10).map(|x| x.to_string())
                     .to_stream(scope);
        });

    }).unwrap();
}
```