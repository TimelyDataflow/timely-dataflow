use timely::dataflow::operators::{Exchange, Input, Probe};
use timely::dataflow::InputHandle;
use timely::Config;

/// A record flushed into an exchange, followed by a step that drains the input's
/// produced counts, must still be delivered when the input later advances.
#[test]
fn gh_824() {
    timely::execute(Config::process(2), |worker| {
        let mut input = InputHandle::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            scope
                .input_from(&mut input)
                .container::<Vec<_>>()
                .exchange(|x: &u64| *x)
                .probe()
                .0
        });

        if worker.index() == 0 {
            input.advance_to(1);
            input.send(0);
            input.flush();
            worker.step();
        }
        input.advance_to(2);

        while probe.less_than(&2) {
            worker.step();
        }

        input.close();
        while !probe.done() {
            worker.step();
        }
    })
    .unwrap();
}
