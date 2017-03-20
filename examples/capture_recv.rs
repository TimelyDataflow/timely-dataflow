extern crate timely;

use std::net::TcpListener;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::capture::{EventReader, Replay};

fn main() {
    timely::execute(timely::Configuration::Thread, |worker| {
        let list = TcpListener::bind("127.0.0.1:8000").unwrap();
        let recv = list.incoming().next().unwrap().unwrap();

        worker.dataflow::<u64,_,_>(|scope| {
            EventReader::<_,u64,_>::new(recv)
                .replay_into(scope)
                .inspect(|x| println!("replayed: {:?}", x));
        })
    }).unwrap(); // asserts error-free execution
}
