extern crate timely;
extern crate timely_communication;

use timely::dataflow::*;
use timely::dataflow::operators::*;

fn main() {
    timely::execute_from_args(std::env::args().skip(1), move |root| {
        let index = root.index();
        let peers = root.peers();

        let mut input = root.scoped::<u64,_,_>(|scope| {

            let (input, stream) = scope.new_input();

            stream
                .broadcast()
                .inspect(move |x| println!("{} -> {:?}", index, x));

            input
        });

        for round in 0u64..10 {
            if (round as usize) % peers == index {
                input.send(round);
            }
            input.advance_to(round + 1);
            root.step();
        }
    }).unwrap();
}
