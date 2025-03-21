use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Input;
use timely::dataflow::InputHandle;
use timely::Config;

#[test] fn operator_scaling_1() { operator_scaling(1); }
#[test] fn operator_scaling_10() { operator_scaling(10); }
#[test] fn operator_scaling_100() { operator_scaling(100); }
#[test] #[cfg_attr(miri, ignore)] fn operator_scaling_1000() { operator_scaling(1000); }
#[test] #[cfg_attr(miri, ignore)] fn operator_scaling_10000() { operator_scaling(10000); }
#[test] #[cfg_attr(miri, ignore)] fn operator_scaling_100000() { operator_scaling(100000); }

fn operator_scaling(scale: u64) {
    timely::execute(Config::thread(), move |worker| {
        let mut input = InputHandle::new();
        worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::Partition;
            let parts =
            scope
                .input_from(&mut input)
                .partition(scale, |()| (0, ()));

            use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
            let mut builder = OperatorBuilder::new("OpScaling".to_owned(), scope.clone());
            let mut handles = Vec::with_capacity(parts.len());
            let mut outputs = Vec::with_capacity(parts.len());
            for (index, part) in parts.into_iter().enumerate() {
                use timely::container::CapacityContainerBuilder;
                let (output, stream) = builder.new_output_connection::<CapacityContainerBuilder<Vec<()>>>(Default::default());
                use timely::progress::Antichain;
                let connectivity = [(index, Antichain::from_elem(Default::default()))].into();
                handles.push((builder.new_input_connection(&part, Pipeline, connectivity), output));
                outputs.push(stream);
            }

            builder.build(move |_| {
                move |_frontiers| {
                    for (input, output) in handles.iter_mut() {
                        let mut output = output.activate();
                        input.for_each(|time, data| {
                            let mut output = output.session_with_builder(&time);
                            for datum in data.drain(..) {
                                output.give(datum);
                            }
                        });
                    }
                }
            });
        });
    })
    .unwrap();
}

#[test] fn subgraph_scaling_1() { subgraph_scaling(1); }
#[test] fn subgraph_scaling_10() { subgraph_scaling(10); }
#[test] fn subgraph_scaling_100() { subgraph_scaling(100); }
#[test] #[cfg_attr(miri, ignore)] fn subgraph_scaling_1000() { subgraph_scaling(1000); }
#[test] #[cfg_attr(miri, ignore)] fn subgraph_scaling_10000() { subgraph_scaling(10000); }
#[test] #[cfg_attr(miri, ignore)] fn subgraph_scaling_100000() { subgraph_scaling(100000); }

fn subgraph_scaling(scale: u64) {
    timely::execute(Config::thread(), move |worker| {
        let mut input = InputHandle::new();
        worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::Partition;
            let parts =
            scope
                .input_from(&mut input)
                .partition(scale, |()| (0, ()));

            use timely::dataflow::Scope;
            let _outputs = scope.region(|inner| {
                use timely::dataflow::operators::{Enter, Leave};
                parts.into_iter().map(|part| part.enter(inner).leave()).collect::<Vec<_>>()
            });
        });
    })
    .unwrap();
}