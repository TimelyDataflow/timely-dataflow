extern crate timely_communication;

use std::ops::Deref;
use timely_communication::{Message, Allocate};

fn main() {

    // use a hardcoded configuration
    let config = timely_communication::CommunicationConfig::Cluster {
        threads: 4,
        process: 0,
        addresses: vec!["localhost:2101".into()],
        report: false,
        log_fn: Box::new(|_| None),
    };
    let guards = timely_communication::initialize(config, |mut allocator| {

        println!("worker {} of {} started", allocator.index(), allocator.peers());

        // allocates a pair of senders list and one receiver.
        let (mut senders, mut receiver) = allocator.allocate(0);

        // send typed data along each channel
        for i in 0 .. allocator.peers() {
            senders[i].send(Message::from_typed(format!("hello, {}", i)));
            senders[i].done();
        }

        // no support for termination notification,
        // we have to count down ourselves.
        let mut received = 0;
        while received < allocator.peers() {

            allocator.receive();

            if let Some(message) = receiver.recv() {
                println!("worker {}: received: <{}>", allocator.index(), message.deref());
                received += 1;
            }

            allocator.release();
        }

        allocator.index()
    });

    // computation runs until guards are joined or dropped.
    if let Ok(guards) = guards {
        for guard in guards.join() {
            println!("result: {:?}", guard);
        }
    }
    else { println!("error in computation"); }
}
