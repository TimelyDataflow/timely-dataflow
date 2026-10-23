//! Input modes preserve data, registration boundaries, and progress.

use std::cell::RefCell;
use std::rc::Rc;

use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::{Input, Inspect, InspectCore, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

#[test]
fn containers_fan_out_at_the_current_time() {
    timely::execute_directly(|worker| {
        // Rc<Vec<_>> needs no element builder.
        let mut input = InputHandle::<u64, Rc<Vec<u64>>>::new();
        input.advance_to(3);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut probes = Vec::new();
        for index in 0..2 {
            let seen = Rc::clone(&seen);
            let mut probe = ProbeHandle::new();
            worker.dataflow(|scope| {
                scope
                    .input_from(&mut input)
                    .inspect_container(move |event| {
                        if let Ok((time, data)) = event {
                            seen.borrow_mut().push((index, *time, (**data).clone()));
                        }
                    })
                    .probe_with(&mut probe);
            });
            probes.push(probe);
        }
        input.send_batch(&mut Rc::new(Vec::new()));
        input.send_batch(&mut Rc::new(vec![1, 2]));
        input.send_batch(&mut Rc::new(vec![3]));
        input.advance_to(4);
        worker.step_while(|| probes.iter().any(|probe| probe.less_than(&4)));
        input.close();
        worker.step_while(|| probes.iter().any(|probe| !probe.done()));
        let mut seen = seen.borrow().clone();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                (0, 3, vec![1, 2]),
                (0, 3, vec![3]),
                (1, 3, vec![1, 2]),
                (1, 3, vec![3])
            ]
        );
    });
}

#[test]
fn wrapping_preserves_streams_and_flushes_on_advance_and_drop() {
    timely::execute_directly(|worker| {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let output = Rc::clone(&seen);
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            stream
                .inspect_batch(move |time, data| {
                    output.borrow_mut().extend(data.iter().map(|x| (*time, *x)));
                })
                .probe_with(&mut probe);
            input
        });
        input.advance_to(2);
        input.send_batch(&mut vec![1]);
        let mut input = input.with_builder::<CapacityContainerBuilder<_>>();
        assert_eq!(input.time(), &2);
        input.send(2);
        input.flush();
        input.send(3);
        input.advance_to(3);
        worker.step_while(|| probe.less_than(&3));
        assert_eq!(*seen.borrow(), vec![(2, 1), (2, 2), (2, 3)]);
        input.send(4);
        drop(input);
        worker.step_while(|| !probe.done());
        assert_eq!(*seen.borrow(), vec![(2, 1), (2, 2), (2, 3), (3, 4)]);
    });
}

#[test]
fn attaching_a_buffered_input_excludes_earlier_records() {
    timely::execute_directly(|worker| {
        let mut input = InputHandle::<u64, Vec<u64>>::new().with_default_builder();
        input.advance_to(5);
        let first = Rc::new(RefCell::new(Vec::new()));
        let second = Rc::new(RefCell::new(Vec::new()));
        let mut probes = Vec::new();
        for seen in [&first, &second] {
            let seen = Rc::clone(seen);
            let mut probe = ProbeHandle::new();
            worker.dataflow(|scope| {
                input
                    .to_stream(scope)
                    .inspect_batch(move |time, data| {
                        seen.borrow_mut().extend(data.iter().map(|x| (*time, *x)));
                    })
                    .probe_with(&mut probe);
            });
            probes.push(probe);
            input.send(probes.len() as u64);
        }
        input.close();
        worker.step_while(|| probes.iter().any(|probe| !probe.done()));
        assert_eq!(*first.borrow(), vec![(5, 1), (5, 2)]);
        assert_eq!(*second.borrow(), vec![(5, 2)]);
    });
}
