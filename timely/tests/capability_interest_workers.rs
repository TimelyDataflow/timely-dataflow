//! An `UpToCapability` operator is woken when the frontier passes a capability it holds, even
//! while another worker's instance of it still holds an earlier capability.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::{Input, Probe};
use timely::progress::operate::FrontierInterest;

#[test]
fn wakes_on_own_capability_ahead_of_peers() {
    // Worker 0 holds its capability at 5 from its first activation. Worker 1 holds its capability
    // at 0 until `release` is set. The input frontier moves 0 -> 3 -> 10, so the move from 3 to 10
    // passes worker 0's capability while the capabilities summed over workers still show 0.
    let release = Arc::new(AtomicBool::new(false));
    let results = timely::execute(timely::Config::process(2), move |worker| {
        let index = worker.index();
        let release = Arc::clone(&release);
        let activator = Rc::new(RefCell::new(None));
        let activator2 = Rc::clone(&activator);
        // Shared so the test can drop a stalled capability and let the dataflow shut down.
        let held = Rc::new(RefCell::new(None));
        let held2 = Rc::clone(&held);
        let (mut input, input_probe, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            let (input_probe, stream) = stream.probe();
            let mut builder = OperatorBuilder::new("Hold".to_owned(), scope);
            let address = builder.operator_info().address;
            let mut input_handle = builder.new_input(stream, Pipeline);
            let (output, output_stream) = builder.new_output::<Vec<u64>>();
            let mut output = OutputBuilder::from(output);
            builder.set_notify_for(0, FrontierInterest::UpToCapability);
            *activator2.borrow_mut() = Some(scope.activator_for(address));
            let release = Arc::clone(&release);
            builder.build(move |mut capabilities| {
                *held2.borrow_mut() = capabilities.pop();
                move |frontiers| {
                    let mut output = output.activate();
                    input_handle.for_each(|time, data| { output.session(&time).give_container(data); });
                    let mut capability = held2.borrow_mut();
                    if let Some(cap) = capability.as_mut() {
                        if *cap.time() == 0 && (index == 0 || release.load(Ordering::SeqCst)) {
                            cap.downgrade(&5);
                        }
                    }
                    if capability.as_ref().is_some_and(|cap| *cap.time() == 5 && !frontiers[0].less_equal(&5)) {
                        *capability = None;
                    }
                }
            });
            let (probe, _) = output_stream.probe();
            (input, input_probe, probe)
        });

        input.advance_to(3);
        while input_probe.less_than(&3) { worker.step(); }
        input.advance_to(10);
        while input_probe.less_than(&10) { worker.step(); }
        for _ in 0 .. 10 { worker.step(); }
        // Worker 0 must have seen the frontier pass its capability and released it, before
        // worker 1 does anything that could schedule it for another reason.
        let released_while_peer_behind = held.borrow().is_none();

        if index == 1 { release.store(true, Ordering::SeqCst); }
        held.borrow_mut().take();
        activator.borrow().as_ref().unwrap().activate();
        input.close();
        while probe.less_than(&10) { worker.step(); }
        (index, released_while_peer_behind)
    }).unwrap().join();

    for result in results {
        let (index, released) = result.unwrap();
        if index == 0 {
            assert!(released, "worker 0 slept through the frontier passing its capability");
        }
    }
}
