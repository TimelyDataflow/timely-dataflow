//! Operator summaries report each input's frontier interest, so a log can audit who is woken by time.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use timely::dataflow::operators::{Enter, Input, Inspect, Leave, Probe};
use timely::dataflow::operators::vec::Map;
use timely::logging::{TimelyEvent, TimelyEventBuilder, TimelySummaryEventBuilder};
use timely::progress::operate::FrontierInterest;

/// Builds `input -> region { map [-> inspect_core] } -> probe`, and returns each operator's name
/// with the frontier interest its summary reports.
fn interests(frontier: bool) -> Vec<(String, Vec<FrontierInterest>)> {
    timely::execute_directly(move |worker| {
        let names: Rc<RefCell<HashMap<usize, String>>> = Default::default();
        let notify: Rc<RefCell<Vec<(usize, Vec<FrontierInterest>)>>> = Default::default();
        let (names2, notify2) = (Rc::clone(&names), Rc::clone(&notify));
        let mut registry = worker.log_register().unwrap();
        registry.insert::<TimelyEventBuilder, _>("timely", move |_time, data| {
            for (_, event) in data.iter().flat_map(|d| d.iter()) {
                if let TimelyEvent::Operates(o) = event { names2.borrow_mut().insert(o.id, o.name.clone()); }
            }
        });
        registry.insert::<TimelySummaryEventBuilder<u64>, _>("timely/summary/u64", move |_time, data| {
            for (_, event) in data.iter().flat_map(|d| d.iter()) {
                notify2.borrow_mut().push((event.id, event.notify.clone()));
            }
        });
        drop(registry);

        let (input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            let stream = scope.region(|inner| {
                let stream = stream.enter(inner).map(|x| x + 1);
                let stream = if frontier { stream.inspect_core(|_| {}) } else { stream };
                stream.leave(scope)
            });
            (input, stream.probe().0)
        });
        drop(input);
        while !probe.done() { worker.step(); }
        worker.log_register().unwrap().remove("timely");
        worker.log_register().unwrap().remove("timely/summary/u64");

        let names = names.borrow();
        let mut result: Vec<_> = notify.borrow().iter().map(|(id, n)| (names[id].clone(), n.clone())).collect();
        result.sort();
        result
    })
}

#[test]
fn summaries_report_frontier_interest() {
    let quiet = interests(false);
    assert!(quiet.contains(&("FlatMap".to_string(), vec![FrontierInterest::Never])), "{quiet:?}");
    // A region is at least `IfCapability`: it may hold capabilities for messages it has yet to acknowledge.
    assert!(quiet.contains(&("Region".to_string(), vec![FrontierInterest::IfCapability])), "{quiet:?}");
    let awake = interests(true);
    assert!(awake.contains(&("Inspect".to_string(), vec![FrontierInterest::Always])), "{awake:?}");
    assert!(awake.contains(&("Region".to_string(), vec![FrontierInterest::Always])), "{awake:?}");
}
