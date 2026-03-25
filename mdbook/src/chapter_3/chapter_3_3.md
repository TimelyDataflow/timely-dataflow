# Operator Execution

Perhaps the most important statement in a timely dataflow program:

```rust
# extern crate timely;
# fn main() {
#     timely::execute_from_args(std::env::args().take(1), |worker| {
worker.step()
#     ;}).unwrap();
# }
```

This is the method that tells the worker that it is now a good time to schedule each of the operators. If you recall, when designing our dataflow we wrote these operators, each of which were programmed by what they would do when shown their input and output handles. This is where we run that code.

The `worker.step()` call is the heart of data processing in timely dataflow. The system will do a swing through each dataflow operator and call in to its closure once. Each operator has the opportunity to drain its input and produce some output, and depending on how they are coded they may do just that.

Importantly, this is also where we start moving data around. Until we call `worker.step()` all data are just sitting in queues. The parts of our computation that do clever things like filtering down the data, or projecting out just a few small fields, or pre-aggregating the data before we act on it, these all happen here and don't happen until we call this.

Make sure to call `worker.step()` now and again, like you would your parents.

## Stepping until caught up

The `worker.step()` method returns `true` if any dataflow operator remains incomplete — meaning it could still receive data or produce output. This includes input operators whose handles have not been dropped. A common mistake is to write:

```rust,ignore
while worker.step() {
    // wait for the dataflow to finish
}
```

This loop will **never terminate** as long as any input handle is still open, because the input operator is always incomplete while it could still receive data. The system has no way to know you are done sending; from its perspective, another `input.send()` could arrive at any moment.

Instead, use a probe to step until the dataflow has caught up to the input:

```rust
# extern crate timely;
# use timely::dataflow::InputHandle;
# use timely::dataflow::operators::{Input, Inspect, Probe};
# fn main() {
#     timely::execute_from_args(std::env::args().take(1), |worker| {
#         let mut input = InputHandle::new();
#         let probe = worker.dataflow(|scope|
#             scope.input_from(&mut input)
#                  .container::<Vec<_>>()
#                  .inspect(|x| println!("seen: {:?}", x))
#                  .probe()
#                  .0
#         );
for round in 0..10 {
    input.send(round);
    input.advance_to(round + 1);
    while probe.less_than(input.time()) {
        worker.step();
    }
}
#     }).unwrap();
# }
```

The probe reports whether there are still timestamps less than the argument that could appear at the probed point in the dataflow. By stepping until `probe.less_than(input.time())` is false, you ensure the dataflow has processed everything up through the current round before moving on.

The `while worker.step()` pattern is only appropriate after all inputs have been closed (by dropping their handles), at which point it correctly runs the dataflow to completion:

```rust,ignore
drop(input);
while worker.step() {
    // drain remaining work after closing input
}
```
