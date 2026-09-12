use timely::{Config, CommunicationConfig, WorkerConfig};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;
use timely::container::CapacityContainerBuilder;

#[test] fn barrier_sync_1w() { barrier_sync_helper(CommunicationConfig::Thread); }
#[test] fn barrier_sync_2w() { barrier_sync_helper(CommunicationConfig::Process(2)); }
#[test] fn barrier_sync_3w() { barrier_sync_helper(CommunicationConfig::Process(3)); }

// This method asserts that each round of execution is notified of at most one time.
fn barrier_sync_helper(comm_config: ::timely::CommunicationConfig) {
    let config = Config {
        communication: comm_config,
        worker: WorkerConfig::default(),
    };
    timely::execute(config, move |worker| {
        worker.dataflow::<usize,_,_>(move |scope| {
            let (handle, stream) = scope.feedback::<Vec<usize>>(1);
            stream.unary_frontier::<CapacityContainerBuilder<_>, _, _, _>(
                Pipeline,
                "Barrier",
                move |capability, _info| {
                    // Capabilities for rounds in flight; each advances once its round is complete.
                    let mut caps = vec![capability.delayed(&0), capability.delayed(&1)];
                    move |(input, frontier), _output| {
                        input.for_each_stamp(|_, _| { });
                        let mut times = std::collections::BTreeSet::new();
                        caps = std::mem::take(&mut caps).into_iter().filter_map(|cap| {
                            if frontier.frontier().less_equal(cap.time()) { Some(cap) } else {
                                times.insert(*cap.time());
                                let time = *cap.time() + 1;
                                if time < 100 { Some(cap.delayed(&time)) } else { None }
                            }
                        }).collect();
                        assert!(times.len() <= 1);
                    }
                }
            )
            .connect_loop(handle);
        });
    }).unwrap(); // asserts error-free execution;
}
