extern crate timely;

use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;

use timely::dataflow::operators::{LoopVariable, ConnectLoop};
use timely::dataflow::operators::generic::unary::Unary;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<usize>().unwrap_or(1_000_000);

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        worker.dataflow(move |scope| {
            let (handle, stream) = scope.loop_variable::<u64>(iterations, 1);
            stream.unary_notify(
                Pipeline,
                "Barrier",
                vec![RootTimestamp::new(0)],
                move |_, _, notificator| {
                    while let Some((cap, _count)) = notificator.next() {
                        let mut time = cap.time().clone();
                        time.inner += 1;
                        if time.inner < iterations {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                }
            )
            .connect_loop(handle);
        });
    }).unwrap(); // asserts error-free execution;
}
