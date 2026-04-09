//! Minimal wasm-bindgen example: a vanilla timely dataflow program that runs
//! in a browser. Mirrors `timely/examples/threadless.rs`, with `println!`
//! replaced by `console::log_1`.
//!
//! Build with `wasm-pack build --target web` from this directory; see
//! `README.md` for details.

use wasm_bindgen::prelude::*;
use web_sys::console;

use timely::WorkerConfig;
use timely::dataflow::operators::{Inspect, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

#[wasm_bindgen(start)]
pub fn run() {
    // Construct a naked single-threaded worker. No timer, no thread spawning,
    // no networking — these are the things that don't work on
    // wasm32-unknown-unknown, and the threadless entry point avoids them all.
    let allocator = timely::communication::allocator::Thread::default();
    let mut worker = timely::worker::Worker::new(WorkerConfig::default(), allocator, None);

    // Create input and probe handles.
    let mut input = InputHandle::new();
    let probe = ProbeHandle::new();

    // Build a small dataflow that logs each record to the browser console.
    worker.dataflow(|scope| {
        input
            .to_stream(scope)
            .container::<Vec<_>>()
            .inspect(|x| console::log_1(&format!("timely says: {:?}", x).into()))
            .probe_with(&probe);
    });

    // Drive the dataflow with a few inputs.
    for i in 0..10 {
        input.send(i);
        input.advance_to(i);
        while probe.less_than(input.time()) {
            worker.step();
        }
    }
}
