extern crate timely;

use std::net::TcpListener;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::capture::{EventReader, Replay};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

        let mut listeners = Vec::new();

        for source_peer in 0 .. source_peers {
            if source_peer % worker.peers() == worker.index() {
                let addr = format!("127.0.0.1:{}", 8000 + source_peer);
                listeners.push(TcpListener::bind(addr).unwrap());
            }
        }

        let mut replayers = Vec::new();
        for listener in listeners {
            let recv = listener.incoming().next().unwrap().unwrap();
            replayers.push(EventReader::<_,u64,_>::new(recv));
        }

        worker.dataflow::<u64,_,_>(|scope| {
            replayers
                .replay_into(scope)
                .inspect(|x| println!("replayed: {:?}", x));
        })
    }).unwrap(); // asserts error-free execution
}
