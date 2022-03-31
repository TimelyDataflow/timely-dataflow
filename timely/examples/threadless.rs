extern crate timely;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Inspect, Probe};
use timely::WorkerConfig;

fn main() {

    // create a naked single-threaded worker.
    let allocator = timely::communication::allocator::Thread::new();
    let kill_switch = Arc::new(AtomicBool::new(false));
    let mut worker = timely::worker::Worker::new(WorkerConfig::default(), allocator, kill_switch);

    // create input and probe handles.
    let mut input = InputHandle::new();
    let mut probe = ProbeHandle::new();

    // directly build a dataflow.
    worker.dataflow(|scope| {
        input
            .to_stream(scope)
            .inspect(|x| println!("{:?}", x))
            .probe_with(&mut probe);
    });

    // manage inputs.
    for i in 0 .. 10 {
        input.send(i);
        input.advance_to(i);
        while probe.less_than(input.time()) {
            worker.step();
        }
    }
}
