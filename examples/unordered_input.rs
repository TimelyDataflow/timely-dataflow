extern crate timely;
extern crate timely_communication;

use timely::dataflow::operators::*;
use timely_communication::Configuration;
use timely::dataflow::{Stream, Scope};
use timely::progress::timestamp::RootTimestamp;

fn main() {
    timely::execute(Configuration::Thread, |root| {
        let (mut input, mut cap) = root.scoped(|scope| {
            let (input, stream) = scope.new_unordered_input();
            stream.inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
            input
        });

        for round in 0..10 {
            input.session(cap.clone()).give(round);
            cap = cap.delayed(&RootTimestamp::new(round + 1));
            root.step();
        }
    }).unwrap();
}
