# Scopes

The first bit of complexity we will introduce is the timely dataflow *scope*.

You can create a new scope in any scope, just by invoking the `scoped` method:

```rust,ignore
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        // create a new nested scope
        scope.scoped(|subscope| {
            // probably want something here
        })

    });
}
```

The main thing that a scope does for you is to add an additional timestamp to all streams that are brought into it. For example, perhaps the timestamp type in your base dataflow scope is `usize`, your nested scope could augment this to `(usize, u32)`, or whatever additional timestamp type you might want.

Why are additional timestamps useful? They allow operators in the nested scope to change their behavior based on the enriched timestamp, without worrying the streams and operators outside the scope about the detail.

For example, the first use of scopes we will see is for *iterative* dataflow. Our nested scopes will introduce a new timestamp corresponding to the round of iteration. This timestamp will allow us to take data in a stream and return it to the beginning of the dataflow, as long as we increment the new timestamp coordinate (the round of iteration). Iterative dataflow is really neat, and in the context of an iterative scope our operators will know that they can observe a round of iteration; outside of the iterative scope the operators have no idea one even exists.

## Entering and exiting scopes

In addition to *creating* scopes, we will also need to get streams of data into and out of scopes.

There are two simple methods, `enter` and `leave`, that allow streams of data into and out of scopes. It is important that you use them! If you try to use a stream in a nested scope, Rust will be confused because it can't get the timestamps of your streams to typecheck. 


```rust,ignore
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        let stream = (0 .. 10).to_stream(scope);

        let result = scope.scoped(|subscope| {
            stream.enter(subscope)
                  .inspect_batch(|t, xs| println!("{:?}, {:?}", t, xs))
                  .leave()
        });

    });
}
```

Notice how we can both `enter` a stream and `leave` in a single sequence of operations. 

The `enter` operator introduces each batch of records as a batch with the same timestamp, extended with a new copy of the default value for the new timestamp. The `leave` just strips off the new timestamp from each batch, resulting in a stream fit for consumption in the containing scope.