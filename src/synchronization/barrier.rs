//! Barrier synchronization.

use ::communication::Allocate;
use progress::timestamp::RootTimestamp;
use progress::nested::product::Product;
use dataflow::{InputHandle, ProbeHandle};
use worker::Worker;

/// A re-usable barrier synchronization mechanism.
pub struct Barrier<A: Allocate> {
    round: usize,
    input: InputHandle<usize, ()>,
    probe: ProbeHandle<Product<RootTimestamp, usize>>,
    worker: Worker<A>,
}

impl<A: Allocate> Barrier<A> {

    /// Allocates a new barrier.
    pub fn new(worker: &mut Worker<A>) -> Self {
        use dataflow::operators::{Input, Probe};
        let (input, probe) = worker.dataflow(|scope| {
            let (handle, stream) = scope.new_input::<()>();
            (handle, stream.probe())
        });
        Barrier { round: 0, input, probe, worker: worker.clone() }
    }

    /// Blocks until all other workers have reached this barrier.
    ///
    /// This method does *not* block dataflow execution, which continues to execute while
    /// we await the arrival of the other workers.
    pub fn wait(&mut self) {
        self.round += 1;
        self.input.advance_to(self.round);
        while self.probe.less_than(self.input.time()) {
            self.worker.step();
        }
    }
}

