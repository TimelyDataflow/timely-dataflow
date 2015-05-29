extern crate timely;

use timely::communication::{ProcessCommunicator, Communicator};
use timely::communication::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::Summary::Local;
use timely::example_static::*;

use std::thread;


fn main() {
    _barrier_multi(1);
}

// #[bench]
// fn barrier_bench(bencher: &mut Bencher) { _barrier(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi(threads: u64) {
    let mut guards = Vec::new();
    for communicator in ProcessCommunicator::new_vector(threads).into_iter() {
        guards.push(thread::spawn(move || _barrier(communicator)));
    }

    for guard in guards { guard.join().unwrap(); }
}

fn _barrier<C: Communicator>(communicator: C) {

    let mut root = GraphRoot::new(communicator);
    {
        let mut graph = root.new_subgraph();
        let (handle, stream) = graph.loop_variable::<u64>(RootTimestamp::new(1_000_000), Local(1));
        stream.enable(graph)
              .unary_notify(Pipeline,
                            format!("Barrier"),
                            vec![RootTimestamp::new(0u64)],
                            |_, _, notificator| {
                  while let Some((mut time, _count)) = notificator.next() {
                      println!("iterating");
                      time.inner += 1;
                      notificator.notify_at(&time);
                  }
              })
              .connect_loop(handle);
    }

    // spin
    while root.step() { }
}
