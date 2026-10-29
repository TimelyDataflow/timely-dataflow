use std::cell::Cell;
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::InputHandle;
use timely::scheduling::current;

/// An operator observes its own global identifier while scheduled, and nothing is current
/// between steps.
#[test]
fn operator_is_current_while_scheduled() {
    timely::execute_directly(|worker| {
        let mut input = InputHandle::new();
        let observed = Rc::new(Cell::new(0usize));
        let expected = Rc::new(Cell::new(None));
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let observed = Rc::clone(&observed);
            let expected = Rc::clone(&expected);
            scope
                .input_from(&mut input)
                .container::<Vec<u64>>()
                .unary(Pipeline, "Observe", move |_cap, info| {
                    expected.set(Some(info.global_id));
                    move |records, output| {
                        assert_eq!(current::operator(), Some(info.global_id));
                        observed.set(observed.get() + 1);
                        records.for_each(|time, data| output.session(&time).give_container(data));
                    }
                })
                .probe()
                .0
        });

        for round in 0..3 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.less_than(&(round + 1)) {
                worker.step();
                assert_eq!(current::operator(), None);
            }
        }
        assert!(expected.get().is_some());
        assert!(observed.get() > 0);
    });
}
