extern crate timely;

use std::fmt::Debug;
use std::hash::Hash;

use timely::Data;
use timely::dataflow::*;
use timely::dataflow::operators::*;

fn main() {
    timely::execute_from_args(std::env::args(), |root| {

        let (mut input1, mut input2) = root.scoped(|graph| {

            // try building some input scopes
            let (input1, stream1) = graph.new_input::<u64>();
            let (input2, stream2) = graph.new_input::<u64>();

            // prepare some feedback edges
            let (loop1_source, loop1) = graph.loop_variable(100, 1);
            let (loop2_source, loop2) = graph.loop_variable(100, 1);

            let concat1 = stream1.concat(&loop1);//graph.concatenate(vec![stream1, loop1]);
            let concat2 = stream2.concat(&loop2);//(&mut graph.concatenate(vec![stream2, loop2]);

            // build up a subgraph using the concatenated inputs/feedbacks
            let (egress1, egress2) = create_subgraph(graph, &concat1, &concat2);

            // connect feedback sources. notice that we have swapped indices ...
            egress1.connect_loop(loop2_source);
            egress2.connect_loop(loop1_source);

            (input1, input2)
        });

        root.step();

        // move some data into the dataflow graph.
        input1.send(0);
        input2.send(1);
    });
}

fn create_subgraph<G: Scope, D>(builder: &G, source1: &Stream<G, D>, source2: &Stream<G, D>) -> (Stream<G, D>, Stream<G, D>)
where D: Data+Hash+Eq+Debug, G::Timestamp: Hash {
    builder.clone().scoped::<u64,_,_>(|subgraph| {
        (subgraph.enter(source1).leave(),
         subgraph.enter(source2).leave())
    })
}
