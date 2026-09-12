use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;
use timely::container::CapacityContainerBuilder;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<usize>().unwrap_or(1_000_000);

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        worker.dataflow::<usize,_,_>(move |scope| {
            let (handle, stream) = scope.feedback::<Vec<usize>>(1);
            stream.unary_frontier::<CapacityContainerBuilder<_>, _, _, _>(
                Pipeline,
                "Barrier",
                move |capability, _info| {
                    // A capability for the current round; advanced once the round is complete.
                    let mut caps = vec![capability.delayed(&0)];
                    move |(input, frontier), _output| {
                        input.for_each_stamp(|_, _| { });
                        caps = std::mem::take(&mut caps).into_iter().filter_map(|cap| {
                            if frontier.frontier().less_equal(cap.time()) { Some(cap) } else {
                                let time = *cap.time() + 1;
                                if time < iterations { Some(cap.delayed(&time)) } else { None }
                            }
                        }).collect();
                    }
                }
            )
            .connect_loop(handle);
        });
    }).unwrap(); // asserts error-free execution;
}
