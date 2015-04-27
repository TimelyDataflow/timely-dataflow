extern crate timely;
use timely::*;
use timely::example_static::inspect::InspectExt;

fn main() {
    // initialize a new computation root
    let mut computation = GraphRoot::new(ThreadCommunicator);

    let mut input = {

        // allocate and use a scoped subgraph builder
        let mut builder = computation.new_subgraph();
        let (input, stream) = builder.new_input();
        stream.enable(builder)
              .inspect(|x| println!("hello {:?}", x));

        input
    };

    for round in 0..10 {
        input.send_at(round, round..round+1);
        input.advance_to(round + 1);
        computation.step();
    }

    input.close();

    while computation.step() { } // finish off any remaining work
}
