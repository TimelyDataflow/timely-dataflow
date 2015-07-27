extern crate timely;

use std::fmt::Debug;
use std::hash::Hash;

use timely::communication::Data;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::Summary::Local;

use timely::construction::*;
use timely::construction::operators::*;

fn main() {
    timely::execute(std::env::args(), |root| {

        let (mut input1, mut input2) = root.subcomputation(|graph| {

            // try building some input scopes
            let (input1, stream1) = graph.new_input::<u64>();
            let (input2, stream2) = graph.new_input::<u64>();

            // prepare some feedback edges
            let (loop1_source, loop1) = graph.loop_variable(RootTimestamp::new(100), Local(1));
            let (loop2_source, loop2) = graph.loop_variable(RootTimestamp::new(100), Local(1));

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
        input1.give(0);
        input2.give(1);

        // see what everyone thinks about that ...
        root.step();

        input1.advance_to(1000000);
        input2.advance_to(1000000);
        input1.close();
        input2.close();

        // spin
        while root.step() { }
    });
}

fn create_subgraph<G: GraphBuilder, D>(builder: &G, source1: &Stream<G, D>, source2: &Stream<G, D>) -> (Stream<G, D>, Stream<G, D>)
where D: Data+Hash+Eq+Debug, G::Timestamp: Hash {
    builder.clone().subcomputation::<u64,_,_>(|subgraph| {
        (subgraph.enter(source1).leave(),
         subgraph.enter(source2).leave())
    })
}
