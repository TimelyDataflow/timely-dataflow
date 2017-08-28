extern crate timely;
extern crate timely_communication;

use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;

use timely::dataflow::operators::{LoopVariable, ConnectLoop};
use timely::dataflow::operators::generic::unary::Unary;

use timely_communication::Configuration;

#[test] fn barrier_sync_1w() { barrier_sync_helper(Configuration::Thread); }
#[test] fn barrier_sync_2w() { barrier_sync_helper(Configuration::Process(2)); }
#[test] fn barrier_sync_3w() { barrier_sync_helper(Configuration::Process(3)); }

// This method asserts that each round of execution is notified of at most one time.
fn barrier_sync_helper(config: ::timely_communication::Configuration) {
    timely::execute(config, move |worker| {
        worker.dataflow(move |scope| {
            let (handle, stream) = scope.loop_variable::<u64>(100, 1);
            stream.unary_notify(
                Pipeline,
                "Barrier",
                vec![RootTimestamp::new(0), RootTimestamp::new(1)],
                move |_, _, notificator| {
                    let mut count = 0;
                    while let Some((cap, _count)) = notificator.next() {
                        count += 1;
                        let mut time = cap.time().clone();
                        time.inner += 1;
                        if time.inner < 100 {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                    assert!(count <= 1);
                }
            )
            .connect_loop(handle);
        });
    }).unwrap(); // asserts error-free execution;
}
