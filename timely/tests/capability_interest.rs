//! A region is woken by frontier changes only at or below the capabilities it holds.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::{Enter, Input, Leave, Probe};
use timely::logging::{TimelyEvent, TimelyEventBuilder, StartStop};
use timely::progress::operate::FrontierInterest;

/// Advances an input through `rounds` rounds without data, while an operator in a region holds a
/// capability at `hold_at` and declares `interest`, and returns how often the region was scheduled.
fn region_schedules(hold_at: u64, interest: FrontierInterest, rounds: u64) -> usize {
    timely::execute_directly(move |worker| {
        let names: Rc<RefCell<HashMap<usize, String>>> = Default::default();
        let starts: Rc<RefCell<HashMap<usize, usize>>> = Default::default();
        let (names2, starts2) = (Rc::clone(&names), Rc::clone(&starts));
        worker.log_register().unwrap().insert::<TimelyEventBuilder, _>("timely", move |_time, data| {
            for (_, event) in data.iter().flat_map(|d| d.iter()) {
                match event {
                    TimelyEvent::Operates(o) => { names2.borrow_mut().insert(o.id, o.name.clone()); }
                    TimelyEvent::Schedule(s) if s.start_stop == StartStop::Start => { *starts2.borrow_mut().entry(s.id).or_default() += 1; }
                    _ => {}
                }
            }
        });

        let held = Rc::new(RefCell::new(None));
        let held2 = Rc::clone(&held);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            let (probe, stream) = stream.probe();
            scope.region(|inner| {
                let mut builder = OperatorBuilder::new("Hold".to_owned(), inner);
                let address = builder.operator_info().address;
                let mut input = builder.new_input(stream.enter(inner), Pipeline);
                let (output, output_stream) = builder.new_output::<Vec<u64>>();
                let mut output = OutputBuilder::from(output);
                builder.set_notify_for(0, interest);
                builder.build(move |mut capabilities| {
                    let capability = capabilities.pop().unwrap().delayed(&hold_at);
                    *held2.borrow_mut() = Some((capability, inner.activator_for(address)));
                    move |_frontiers| {
                        let mut output = output.activate();
                        input.for_each(|time, data| { output.session(&time).give_container(data); });
                    }
                });
                output_stream.leave(scope);
            });
            (input, probe)
        });

        for round in 1 ..= rounds {
            input.advance_to(round);
            while probe.less_than(&round) { worker.step(); }
        }
        let region = names.borrow().iter().find(|(_, n)| *n == "Region").map(|(id, _)| *id).expect("region logged");
        let count = starts.borrow().get(&region).copied().unwrap_or(0);

        let (capability, activator) = held.borrow_mut().take().unwrap();
        drop(capability);
        activator.activate();
        drop(input);
        while worker.step() { }
        worker.log_register().unwrap().remove("timely");
        count
    })
}

#[test]
fn passed_capability_lets_region_sleep() {
    let rounds = 100;
    let count = region_schedules(0, FrontierInterest::Never, rounds);
    assert!(count < 10, "a region holding a passed capability should sleep as frontiers move: {count}");
}

#[test]
fn capability_ahead_wakes_region() {
    let rounds = 100;
    let count = region_schedules(rounds + 1, FrontierInterest::Never, rounds);
    assert!(count >= rounds as usize, "a region holding a capability ahead of the frontier should wake each round: {count}");
}

#[test]
fn if_capability_child_wakes_region() {
    let rounds = 100;
    let count = region_schedules(0, FrontierInterest::IfCapability, rounds);
    assert!(count >= rounds as usize, "an `IfCapability` child should wake its region each round: {count}");
}
