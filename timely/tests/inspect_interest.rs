//! `inspect` does not observe frontiers, so it does not wake an enclosing region when they move.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use timely::dataflow::operators::{Enter, Input, Inspect, Leave, Probe};
use timely::logging::{TimelyEvent, TimelyEventBuilder, StartStop};

/// Advances an input through `rounds` rounds without data, and returns how often the region
/// between the input and the probe was scheduled. `frontier` selects `inspect_core` over `inspect`.
fn region_schedules(frontier: bool, rounds: u64) -> usize {
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

        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            let stream = scope.region(|inner| {
                let stream = stream.enter(inner);
                let stream = if frontier { stream.inspect_core(|_| {}) } else { stream.inspect(|_| {}) };
                stream.leave(scope)
            });
            (input, stream.probe().0)
        });

        for round in 1 ..= rounds {
            input.advance_to(round);
            while probe.less_than(&round) { worker.step(); }
        }
        drop(input);
        while worker.step() { }
        worker.log_register().unwrap().remove("timely");

        let region = names.borrow().iter().find(|(_, n)| *n == "Region").map(|(id, _)| *id).expect("region logged");
        let count = starts.borrow().get(&region).copied().unwrap_or(0);
        count
    })
}

#[test]
fn inspect_lets_region_sleep() {
    let rounds = 100;
    let sleepy = region_schedules(false, rounds);
    let awake = region_schedules(true, rounds);
    assert!(awake >= rounds as usize, "inspect_core should wake the region each round: {awake}");
    assert!(sleepy < 10, "inspect should not wake the region as frontiers move: {sleepy}");
}
