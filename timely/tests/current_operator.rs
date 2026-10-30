use std::cell::Cell;
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::InputHandle;
use timely::scheduling::current;

/// An operator observes its own global identifier while scheduled, also inside a nested region,
/// and nothing is current between steps.
#[test]
fn operator_is_current_while_scheduled() {
    timely::execute_directly(|worker| {
        let mut input = InputHandle::new();
        let outer_runs = Rc::new(Cell::new(0usize));
        let inner_runs = Rc::new(Cell::new(0usize));
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let outer_observed = Rc::clone(&outer_runs);
            let inner_observed = Rc::clone(&inner_runs);
            let stream = scope
                .input_from(&mut input)
                .container::<Vec<u64>>()
                .unary(Pipeline, "Outer", move |_cap, info| {
                    move |records, output| {
                        assert_eq!(current::operator_id(), Some(info.global_id));
                        outer_observed.set(outer_observed.get() + 1);
                        records.for_each(|time, data| output.session(&time).give_container(data));
                    }
                });
            scope
                .region(|inner| {
                    use timely::dataflow::operators::{Enter, Leave};
                    stream
                        .enter(inner)
                        .unary(Pipeline, "Inner", move |_cap, info| {
                            move |records, output| {
                                // The innermost operator, not the region scheduling it.
                                assert_eq!(current::operator_id(), Some(info.global_id));
                                inner_observed.set(inner_observed.get() + 1);
                                records.for_each(|time, data| {
                                    output.session(&time).give_container(data)
                                });
                            }
                        })
                        .leave(scope)
                })
                .probe()
                .0
        });

        for round in 0..3 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.less_than(&(round + 1)) {
                worker.step();
                assert_eq!(current::operator_id(), None);
            }
        }
        assert!(outer_runs.get() > 0 && inner_runs.get() > 0, "both operators ran");
    });
}
