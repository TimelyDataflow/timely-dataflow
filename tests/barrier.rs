extern crate timely;

use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;

use timely::dataflow::operators::{LoopVariable, ConnectLoop};
use timely::dataflow::operators::generic::unary::Unary;

#[test]
fn barrier_sync() {
    timely::example(|scope| {
        let (handle, stream) = scope.loop_variable::<u64>(100, 1);
        stream.unary_notify(
            Pipeline,
            "Barrier",
            vec![RootTimestamp::new(0), RootTimestamp::new(1)],
            move |_, _, notificator| {
                let mut once = true;
                while let Some((cap, _count)) = notificator.next() {
                    assert!(once);
                    once = false;
                    let mut time = cap.time().clone();
                    time.inner += 1;
                    if time.inner < 100 {
                        notificator.notify_at(cap.delayed(&time));
                    }
                }
            }
        )
        .connect_loop(handle);
    }); // asserts error-free execution;
}
